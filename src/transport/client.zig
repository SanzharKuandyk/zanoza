const std = @import("std");
const TransportClientError = @import("error.zig").TransportClientError;
const root = @import("../root.zig");
const validation = @import("../validation/root.zig");
const packet_mod = @import("../packet.zig");
const client_mod = @import("../client/client.zig");
const channel_mod = @import("../channel.zig");
const state_mod = @import("../channel_state.zig");
const socket_mod = @import("socket.zig");
const RingQueue = @import("../ring_buffer.zig").RingQueue;
const AddressKey = @import("../addr.zig").AddressKey;
const UdpSocket = @import("udp.zig").UdpSocket;

pub fn TransportClient(comptime opts: root.Options, comptime SocketType: type) type {
    const Socket = if (SocketType == void) UdpSocket else SocketType;

    comptime {
        validation.options.validate(opts);
        if (SocketType != void) validation.socket.validate(Socket);
    }

    const Cli = client_mod.Client(opts);
    const max_packet_size = packet_mod.maxPacketSize(opts);
    const max_payload_size = opts.max_payload_size;
    const PoolRef = Cli.PayloadPool.Ref;

    const Layout = channel_mod.ChannelLayout(opts.channels);
    const channel_count = Layout.channel_count;

    const States = state_mod.States(opts.channels, opts.max_payload_size, .embedded);
    const initial_states = state_mod.initStates(opts.channels, opts.max_payload_size, .embedded);

    const ConnectTokenType = if (opts.ConnectToken == void)
        @import("../handshake.zig").DefaultConnectToken(opts.user_data_size, opts.max_token_addresses)
    else
        opts.ConnectToken;

    return struct {
        const Self = @This();
        pub const Error = TransportClientError;
        /// Constructor errors include the selected socket implementation's open errors.
        pub const InitError = Error || socket_mod.OpenError(Socket);

        fn ChannelState(comptime channel_id: u8) type {
            return state_mod.ChannelState(opts.channels[channel_id], opts.max_payload_size, .embedded);
        }

        pub const max_message_size = max_payload_size;
        pub const Event = enum { Connected, Disconnected };

        pub const Message = struct {
            channel_id: u8,
            data: [max_payload_size]u8,
            len: usize,
        };

        /// Payload is either a reference into the SM pool (non-fragmented) or a
        /// heap-allocated buffer (assembled from fragments).
        pub const MessagePayload = union(enum) {
            pooled: PoolRef,
            assembled: []u8,
        };

        pub const MessageView = struct {
            channel_id: u8,
            payload: MessagePayload,
        };

        cli: Cli,
        socket: Socket,
        allocator: std.mem.Allocator,
        channel_states: States,
        rtt_state: channel_mod.RttState,
        events: RingQueue(Event, opts.events_queue_size),
        messages: RingQueue(MessageView, opts.messages_queue_size),

        pub fn init(config: root.ClientConfig, bind_addr: root.Address) InitError!Self {
            const socket = try Socket.open(bind_addr);
            errdefer {
                var s = socket;
                s.close();
            }
            return initWithSocket(config, socket);
        }

        pub fn initWithSocket(config: root.ClientConfig, socket: Socket) TransportClientError!Self {
            const cli = try Cli.init(config);
            return .{
                .cli = cli,
                .socket = socket,
                .allocator = std.heap.page_allocator,
                .channel_states = initial_states,
                .rtt_state = .{},
                .events = .{},
                .messages = .{},
            };
        }

        pub fn deinit(self: *Self) void {
            self.clearMessages();
            self.resetChannelState();
            self.socket.close();
            self.cli.deinit();
        }

        pub fn tick(self: *Self) void {
            self.cli.updateNow();
            const now_ns = self.cli.getCurrentTime();

            self.flushOutgoing();

            var buf: [max_packet_size]u8 = undefined;
            while (self.socket.recvfrom(&buf)) |result| {
                if (!AddressKey.eql(
                    AddressKey.fromAddress(result.addr),
                    AddressKey.fromAddress(self.cli.config.server_addr),
                )) continue;

                self.cli.handlePacket(buf[0..result.len]) catch {};
            }

            while (self.cli.peekEvent()) |ev| {
                switch (ev.*) {
                    .Connected => {
                        self.resetChannelState();
                        _ = self.events.pushBack(.Connected);
                    },
                    .Disconnected => {
                        self.resetChannelState();
                        _ = self.events.pushBack(.Disconnected);
                    },
                }
                self.cli.consumeEvent();
            }

            // Drain SM message queue (payloads from wire -> pool).
            while (self.cli.peekMessage()) |raw| {
                const ref = raw.payload;
                const header = raw.header;
                self.cli.consumeMessage();
                self.handleIncoming(header, ref);
            }

            self.flushOutgoing();
            self.retransmitReliable(now_ns);
            self.flushOutgoing();
        }

        fn flushOutgoing(self: *Self) void {
            var buf: [max_packet_size]u8 = undefined;
            while (self.cli.peekOutgoing()) |out| {
                const len = packet_mod.serialize(opts, out.packet, buf[0..]) catch {
                    self.cli.consumeOutgoing();
                    continue;
                };
                self.socket.sendto(out.addr, buf[0..len]);
                self.cli.consumeOutgoing();
            }
        }

        fn handleIncoming(self: *Self, hdr: packet_mod.PayloadHeader, ref: PoolRef) void {
            inline for (opts.channels, 0..) |_, index| {
                if (hdr.channel_id == index) return self.handleChannelIncoming(@intCast(index), hdr, ref);
            }
            self.cli.releasePayload(ref);
        }

        fn handleChannelIncoming(self: *Self, comptime channel_id: u8, hdr: packet_mod.PayloadHeader, ref: PoolRef) void {
            const body = self.cli.payloadData(ref);
            const ch_idx = channel_id;
            if (ch_idx >= channel_count) {
                self.cli.releasePayload(ref);
                return;
            }
            const kind = comptime Layout.kind(ch_idx);
            if (hdr.is_ack) {
                self.handleAck(ch_idx, kind, hdr.seq);
                self.cli.releasePayload(ref);
                return;
            }

            switch (kind) {
                .Unreliable => {},
                .UnreliableLatest => {
                    if (!self.acceptLatest(ch_idx, hdr.seq)) {
                        self.cli.releasePayload(ref);
                        return;
                    }
                },
                .ReliableOrdered => {
                    self.handleReliableOrdered(ch_idx, hdr.seq, hdr.fragment, ref);
                    return;
                },
                .ReliableUnordered => {
                    if (!self.acceptReliableUnordered(ch_idx, hdr.seq)) {
                        self.cli.releasePayload(ref);
                        return;
                    }
                },
            }

            if (hdr.fragment) |fragment| {
                self.handleFragmentBytes(ch_idx, fragment, body);
                self.cli.releasePayload(ref);
            } else {
                self.enqueueMessage(ch_idx, .{ .pooled = ref });
            }
        }

        fn retransmitReliable(self: *Self, now_ns: u64) void {
            const rto = self.rtt_state.rto(opts.reliable_resend_ns);
            inline for (opts.channels, 0..) |config, channel_id| {
                switch (config.kind) {
                    .ReliableOrdered, .ReliableUnordered => {
                        self.retransmitState(&self.channel_states[channel_id].send, @intCast(channel_id), now_ns, rto);
                    },
                    else => {},
                }
            }
        }

        fn retransmitState(self: *Self, rel: anytype, comptime channel_id: u8, now_ns: u64, rto: u64) void {
            for (&rel.entries) |*e| {
                if (!e.active) continue;
                if (now_ns -| e.sent_at < rto) continue;

                const out = self.cli.reservePayloadSlot() catch continue;
                out.packet.Payload.header = .{
                    .channel_id = channel_id,
                    .seq = e.seq,
                    .fragment = e.fragment,
                };
                @memcpy(out.packet.Payload.body[0..e.len], e.data[0..e.len]);
                out.packet.Payload.len = @intCast(e.len);
                e.sent_at = now_ns;
            }
        }

        pub fn sendOnChannel(self: *Self, channel_id: u8, data: []const u8) TransportClientError!void {
            inline for (opts.channels, 0..) |_, index| {
                if (channel_id == index) return self.sendOnConfiguredChannel(@intCast(index), data);
            }
            return error.InvalidChannel;
        }

        fn sendOnConfiguredChannel(self: *Self, comptime channel_id: u8, data: []const u8) TransportClientError!void {
            if (channel_id >= channel_count) return error.InvalidChannel;
            if (self.cli.state != .Connected) return error.InvalidState;

            const config = comptime Layout.config(channel_id);
            if (config.frag_config) |cfg| {
                if (cfg.policy == .always or data.len > max_payload_size) {
                    return self.sendFragmented(channel_id, data, cfg.fragment_size);
                }
            }
            if (data.len > max_payload_size) return error.MessageTooLarge;

            const now_ns = self.cli.getCurrentTime();
            var send_state_ptr: ?*ChannelState(channel_id).Send = null;
            var seq: u16 = 0;

            switch (config.kind) {
                .Unreliable => {},
                .UnreliableLatest => {
                    const state = &self.channel_states[channel_id];
                    state.send_seq +%= 1;
                    seq = state.send_seq;
                },
                .ReliableOrdered => {
                    const send = self.orderedSendState(channel_id) orelse return error.InvalidChannel;
                    if (send.isNextNotEmpty()) return error.ReliableBufferFull;
                    send_state_ptr = send;
                },
                .ReliableUnordered => {
                    const send = self.unorderedSendState(channel_id) orelse return error.InvalidChannel;
                    if (send.isNextNotEmpty()) return error.ReliableBufferFull;
                    send_state_ptr = send;
                },
            }

            // Commit the reliable entry only after reserving the outgoing slot.
            const outgoing = try self.cli.reservePayloadSlot();
            if (comptime config.kind == .ReliableOrdered or config.kind == .ReliableUnordered) {
                if (send_state_ptr) |send| {
                    seq = send.push(data, now_ns) orelse unreachable;
                }
            }

            outgoing.packet.Payload.header = .{
                .channel_id = channel_id,
                .seq = seq,
            };
            @memcpy(outgoing.packet.Payload.body[0..data.len], data);
            outgoing.packet.Payload.len = @intCast(data.len);
        }

        fn sendFragmented(self: *Self, comptime channel_id: u8, data: []const u8, fragment_size: usize) TransportClientError!void {
            if (self.cli.state != .Connected) return error.InvalidState;

            // would have to increase `total_size` and `frag_count` field
            // for fragments if more size needed
            if (data.len > std.math.maxInt(u16)) return error.MessageTooLarge;
            const fragment_count = if (data.len == 0) 1 else (data.len - 1) / fragment_size + 1;
            if (fragment_count > std.math.maxInt(u8)) return error.MessageTooLarge;

            const fragment_state = self.fragChannelPtr(channel_id) orelse return error.InvalidChannel;
            const reliable_send = self.reliableSendState(channel_id);
            // Admit the complete message before changing IDs, sequences, or queue lengths.
            if (comptime opts.channels[channel_id].kind == .ReliableOrdered or opts.channels[channel_id].kind == .ReliableUnordered) {
                if (reliable_send) |send| {
                    if (!send.canPush(fragment_count)) return error.ReliableBufferFull;
                }
            }
            if (fragment_count > opts.outgoing_queue_size - self.cli.outgoing.len) return error.IoError;
            fragment_state.next_msg_id +%= 1;
            const now_ns = self.cli.getCurrentTime();
            for (0..fragment_count) |index| {
                const offset = index * fragment_size;
                const chunk = data[offset..@min(offset + fragment_size, data.len)];
                const fragment: packet_mod.FragmentHeader = .{
                    .msg_id = fragment_state.next_msg_id,
                    .frag_count = @intCast(fragment_count),
                    .frag_index = @intCast(index),
                    .total_size = @intCast(data.len),
                };
                const outgoing = self.cli.reservePayloadSlot() catch unreachable;
                var seq: u16 = 0;
                if (comptime opts.channels[channel_id].kind == .ReliableOrdered or opts.channels[channel_id].kind == .ReliableUnordered) {
                    if (reliable_send) |send| {
                        seq = send.pushFragment(chunk, now_ns, fragment) orelse unreachable;
                    }
                }
                outgoing.packet.Payload.header = .{
                    .channel_id = channel_id,
                    .seq = seq,
                    .fragment = fragment,
                };
                @memcpy(outgoing.packet.Payload.body[0..chunk.len], chunk);
                outgoing.packet.Payload.len = @intCast(chunk.len);
            }
        }

        pub fn connect(self: *Self) TransportClientError!void {
            try self.cli.connect();
        }

        pub fn connectSecure(self: *Self, token: ConnectTokenType) TransportClientError!void {
            try self.cli.connectSecure(token);
        }

        pub fn disconnect(self: *Self) void {
            self.cli.disconnect();
        }

        pub fn pollEvent(self: *Self) ?Event {
            // Copy-out convenience API.
            return self.events.popFront();
        }

        pub fn pollMessage(self: *Self) ?Message {
            // Copy-out convenience API built on top of the zero-copy message view.
            // Note: assembled (fragmented) messages are truncated to max_payload_size bytes.
            // Use peekMessage/messageData/consumeMessage for full access to large messages.
            const msg = self.peekMessage() orelse return null;
            const data = self.messageData(msg);
            const copy_len = @min(data.len, max_payload_size);
            var owned: Message = .{
                .channel_id = msg.channel_id,
                .data = undefined,
                .len = copy_len,
            };
            @memcpy(owned.data[0..copy_len], data[0..copy_len]);
            self.consumeMessage();
            return owned;
        }

        pub fn peekMessage(self: *const Self) ?*const MessageView {
            // Zero-copy view into the front message. Read bytes with messageData(msg).
            return self.messages.peekFront();
        }

        pub fn messageData(self: *const Self, msg: *const MessageView) []const u8 {
            return switch (msg.payload) {
                .pooled => |ref| self.cli.payloadData(ref),
                .assembled => |data| data,
            };
        }

        pub fn consumeMessage(self: *Self) void {
            const msg = self.messages.peekFront() orelse return;
            switch (msg.payload) {
                .pooled => |ref| self.cli.releasePayload(ref),
                .assembled => |data| self.allocator.free(data),
            }
            self.messages.advance();
        }

        pub fn getStateMachine(self: *Self) *Cli {
            return &self.cli;
        }

        fn sendAck(self: *Self, comptime channel_id: u8, seq: u16) void {
            const out = self.cli.reservePayloadSlot() catch return;
            out.packet.Payload.header = .{ .channel_id = channel_id, .seq = seq, .is_ack = true };
        }

        fn handleAck(self: *Self, comptime channel_id: u8, comptime kind: channel_mod.ChannelKind, seq: u16) void {
            const now_ns = self.cli.getCurrentTime();
            const sent_at: ?u64 = switch (kind) {
                .ReliableOrdered => blk: {
                    const send = self.orderedSendState(channel_id) orelse break :blk null;
                    break :blk send.ack(seq);
                },
                .ReliableUnordered => blk: {
                    const send = self.unorderedSendState(channel_id) orelse break :blk null;
                    break :blk send.ack(seq);
                },
                else => null,
            };
            if (sent_at) |t| {
                self.rtt_state.update(now_ns -| t);
            }
        }

        fn acceptLatest(self: *Self, comptime channel_id: u8, seq: u16) bool {
            const recv = self.ulRecvState(channel_id) orelse return false;
            return recv.accept(seq);
        }

        fn handleReliableOrdered(
            self: *Self,
            comptime channel_id: u8,
            seq: u16,
            fragment: ?packet_mod.FragmentHeader,
            ref: PoolRef,
        ) void {
            const body = self.cli.payloadData(ref);
            const recv = self.orderedRecvState(channel_id) orelse {
                self.cli.releasePayload(ref);
                return;
            };
            const action = recv.receiveFragment(self.allocator, seq, body, fragment) catch {
                self.cli.releasePayload(ref);
                return;
            };

            if (action == .drop) {
                self.cli.releasePayload(ref);
                return;
            }

            self.sendAck(channel_id, seq);
            if (action == .ack_only) {
                self.cli.releasePayload(ref);
                return;
            }
            if (fragment) |frag| {
                self.handleFragmentBytes(channel_id, frag, body);
                self.cli.releasePayload(ref);
            } else {
                self.enqueueMessage(channel_id, .{ .pooled = ref });
            }

            while (recv.peekReady()) |ready_body| {
                if (recv.readyFragment()) |frag| {
                    self.handleFragmentBytes(channel_id, frag, ready_body);
                } else {
                    const future_ref = self.cli.payload_pool.allocCopy(ready_body) orelse break;
                    self.enqueueMessage(channel_id, .{ .pooled = future_ref });
                }
                recv.consumeReady();
            }
        }

        fn handleFragmentBytes(self: *Self, comptime channel_id: u8, frag: packet_mod.FragmentHeader, chunk: []const u8) void {
            if (comptime opts.channels[channel_id].frag_config == null) return;
            const frag_sz = if (Layout.config(channel_id).frag_config) |cfg| cfg.fragment_size else return;
            const frag_ch = self.fragChannelPtr(channel_id) orelse return;
            const slot_idx = @as(usize, frag.msg_id) % frag_ch.slots.len;
            const ab = &frag_ch.slots[slot_idx];

            if (ab.active and ab.msg_id != frag.msg_id) {
                ab.deinit(self.allocator);
            }

            if (!ab.active) {
                const data = self.allocator.alloc(u8, frag.total_size) catch return;
                ab.* = state_mod.AssemblyBuf{
                    .msg_id = frag.msg_id,
                    .frag_count = frag.frag_count,
                    .frags_received = 0,
                    .total_size = frag.total_size,
                    .data = data,
                    .received_mask = @as([32]u8, @splat(0)),
                    .active = true,
                };
            }

            if (frag.frag_count != ab.frag_count or frag.total_size != ab.total_size) return;
            if (ab.hasFrag(frag.frag_index)) return; // duplicate

            ab.markFrag(frag.frag_index);
            ab.frags_received += 1;

            const chunk_offset = @as(usize, frag.frag_index) * frag_sz;
            const assembled_data = ab.data.?;
            if (chunk_offset < assembled_data.len) {
                const copy_len = @min(chunk.len, assembled_data.len - chunk_offset);
                @memcpy(assembled_data[chunk_offset .. chunk_offset + copy_len], chunk[0..copy_len]);
            }

            if (ab.frags_received == ab.frag_count) {
                const assembled = ab.data.?;
                ab.* = .{};
                self.enqueueMessage(channel_id, .{ .assembled = assembled });
            }
        }

        fn acceptReliableUnordered(self: *Self, comptime channel_id: u8, seq: u16) bool {
            const recv = self.unorderedRecvStatePtr(channel_id) orelse return false;
            const action = recv.classify(seq);
            if (action != .stale) self.sendAck(channel_id, seq);
            return action == .deliver;
        }

        fn enqueueMessage(self: *Self, channel_id: u8, payload: MessagePayload) void {
            if (!self.messages.pushBack(.{
                .channel_id = channel_id,
                .payload = payload,
            })) {
                switch (payload) {
                    .pooled => |ref| self.cli.releasePayload(ref),
                    .assembled => |data| self.allocator.free(data),
                }
            }
        }

        fn reliableSendState(self: *Self, comptime channel_id: u8) ?*ChannelState(channel_id).Send {
            return switch (opts.channels[channel_id].kind) {
                .ReliableOrdered => self.orderedSendState(channel_id),
                .ReliableUnordered => self.unorderedSendState(channel_id),
                else => null,
            };
        }

        fn orderedSendState(self: *Self, comptime channel_id: u8) ?*ChannelState(channel_id).Send {
            if (comptime opts.channels[channel_id].kind != .ReliableOrdered) return null;
            return &self.channel_states[channel_id].send;
        }

        fn unorderedSendState(self: *Self, comptime channel_id: u8) ?*ChannelState(channel_id).Send {
            if (comptime opts.channels[channel_id].kind != .ReliableUnordered) return null;
            return &self.channel_states[channel_id].send;
        }

        fn orderedRecvState(self: *Self, comptime channel_id: u8) ?*ChannelState(channel_id).Recv {
            if (comptime opts.channels[channel_id].kind != .ReliableOrdered) return null;
            return &self.channel_states[channel_id].recv;
        }

        fn unorderedRecvStatePtr(self: *Self, comptime channel_id: u8) ?*ChannelState(channel_id).Recv {
            if (comptime opts.channels[channel_id].kind != .ReliableUnordered) return null;
            return &self.channel_states[channel_id].recv;
        }

        fn ulRecvState(self: *Self, comptime channel_id: u8) ?*channel_mod.UnreliableLatestState {
            if (comptime opts.channels[channel_id].kind != .UnreliableLatest) return null;
            return &self.channel_states[channel_id].recv;
        }

        fn fragChannelPtr(self: *Self, comptime channel_id: u8) ?*ChannelState(channel_id).Fragments {
            if (comptime opts.channels[channel_id].frag_config == null) return null;
            return &self.channel_states[channel_id].fragments;
        }

        fn resetChannelState(self: *Self) void {
            inline for (opts.channels, 0..) |_, index| {
                self.channel_states[index].deinit(self.allocator);
            }
            self.rtt_state = .{};
        }

        fn clearMessages(self: *Self) void {
            while (self.messages.peekFront()) |msg| {
                switch (msg.payload) {
                    .pooled => |ref| self.cli.releasePayload(ref),
                    .assembled => |data| self.allocator.free(data),
                }
                self.messages.advance();
            }
        }
    };
}
