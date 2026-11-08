const std = @import("std");
const root = @import("../root.zig");
const validation = @import("../validation/root.zig");
const packet_mod = @import("../packet.zig");
const client_mod = @import("../client/client.zig");
const channel_mod = @import("../channel.zig");
const socket_mod = @import("socket.zig");
const RingQueue = @import("../ring_buffer.zig").RingQueue;
const AddressKey = @import("../addr.zig").AddressKey;
const UdpSocket = @import("udp.zig").UdpSocket;

/// How many simultaneous in-flight fragmented messages per channel we can reassemble.
const FRAG_ASSEMBLY_SLOTS: usize = 8;

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

    const max_rel_buf = comptime blk: {
        var m: usize = 1;
        for (opts.channels) |ch| {
            if (ch.kind == .ReliableOrdered or ch.kind == .ReliableUnordered)
                m = @max(m, ch.reliable_buffer);
        }
        break :blk m;
    };
    const max_ordered_recv_window = comptime blk: {
        var m: usize = 0;
        for (opts.channels) |ch| {
            if (ch.kind == .ReliableOrdered)
                m = @max(m, ch.ordered_recv_window);
        }
        break :blk m;
    };
    const max_unordered_recv_window = comptime blk: {
        var m: usize = 1;
        for (opts.channels) |ch| {
            if (ch.kind == .ReliableUnordered)
                m = @max(m, ch.unordered_recv_window);
        }
        break :blk m;
    };

    const RelState = channel_mod.ReliableState(max_rel_buf, max_payload_size);
    const OrderedRecvState = channel_mod.ReliableOrderedRecvState(
        max_ordered_recv_window,
        opts.max_payload_size,
    );
    const UnorderedRecvState = channel_mod.ReliableUnorderedRecvState(max_unordered_recv_window);

    const OrderedCount = Layout.ordered_count;
    const UnorderedCount = Layout.unordered_count;
    const UlCount = Layout.latest_count;
    const Frag = Layout.frag_count;

    const ConnectTokenType = if (opts.ConnectToken == void)
        @import("../handshake.zig").DefaultConnectToken(opts.user_data_size, opts.max_token_addresses)
    else
        opts.ConnectToken;

    const OrderedChannelState = struct {
        send: RelState = .{},
        recv: OrderedRecvState = .{},
    };

    const UnorderedChannelState = struct {
        send: RelState = .{},
        recv: UnorderedRecvState = .{},
    };

    const UlChannelState = struct {
        send_seq: u16 = 0,
        recv: channel_mod.UnreliableLatestState = .{},
    };

    // Per-channel fragment reassembly slot.
    const AssemblyBuf = struct {
        msg_id: u16 = 0,
        frag_count: u8 = 0,
        frags_received: u8 = 0,
        total_size: u16 = 0,
        data: ?[]u8 = null,
        received_mask: [32]u8 = @splat(0),
        active: bool = false,

        fn hasFrag(self: *const @This(), fi: u8) bool {
            const bit: u3 = @intCast(fi % 8);
            return (self.received_mask[fi / 8] >> bit) & 1 != 0;
        }

        fn markFrag(self: *@This(), fi: u8) void {
            const bit: u3 = @intCast(fi % 8);
            self.received_mask[fi / 8] |= @as(u8, 1) << bit;
        }

        fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            if (self.data) |d| allocator.free(d);
            self.* = .{};
        }
    };

    const FragChannelState = struct {
        next_msg_id: u16 = 0,
        slots: [FRAG_ASSEMBLY_SLOTS]AssemblyBuf = @splat(.{}),

        fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            for (&self.slots) |*s| s.deinit(allocator);
            self.* = .{};
        }
    };

    return struct {
        const Self = @This();

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
        // State is split by channel kind so each channel only keeps what it uses.
        ordered_state: [OrderedCount]OrderedChannelState,
        unordered_state: [UnorderedCount]UnorderedChannelState,
        ul_state: [UlCount]UlChannelState,
        rtt_state: channel_mod.RttState,
        // Inline frag state; zero-sized ([0]FragChannelState) when no channels use fragmentation.
        frag_state: [Frag]FragChannelState,
        events: RingQueue(Event, opts.events_queue_size),
        messages: RingQueue(MessageView, opts.messages_queue_size),

        pub fn init(config: root.ClientConfig, bind_addr: root.Address) !Self {
            const socket = try Socket.open(bind_addr);
            errdefer {
                var s = socket;
                s.close();
            }
            return initWithSocket(config, socket);
        }

        pub fn initWithSocket(config: root.ClientConfig, socket: Socket) !Self {
            const cli = try Cli.init(config);
            return .{
                .cli = cli,
                .socket = socket,
                .allocator = std.heap.page_allocator,
                .ordered_state = @as([OrderedCount]OrderedChannelState, @splat(.{})),
                .unordered_state = @as([UnorderedCount]UnorderedChannelState, @splat(.{})),
                .ul_state = @as([UlCount]UlChannelState, @splat(.{})),
                .rtt_state = .{},
                .frag_state = @as([Frag]FragChannelState, @splat(.{})),
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
            const body = self.cli.payloadData(ref);
            const ch_idx = hdr.channel_id;
            if (ch_idx >= channel_count) {
                self.cli.releasePayload(ref);
                return;
            }
            const kind = Layout.kind(ch_idx);
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
            for (0..channel_count) |ch_idx| {
                switch (Layout.kind(@intCast(ch_idx))) {
                    .ReliableOrdered => {
                        const send = self.orderedSendState(@intCast(ch_idx)) orelse continue;
                        self.retransmitState(send, @intCast(ch_idx), now_ns, rto);
                    },
                    .ReliableUnordered => {
                        const send = self.unorderedSendState(@intCast(ch_idx)) orelse continue;
                        self.retransmitState(send, @intCast(ch_idx), now_ns, rto);
                    },
                    else => {},
                }
            }
        }

        fn retransmitState(self: *Self, rel: *RelState, channel_id: u8, now_ns: u64, rto: u64) void {
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

        pub fn sendOnChannel(self: *Self, channel_id: u8, data: []const u8) !void {
            if (channel_id >= channel_count) return error.InvalidChannel;
            if (self.cli.state != .Connected) return error.InvalidState;

            const config = Layout.config(channel_id);
            if (config.fragment_size) |fragment_size| {
                if (config.fragmentation_policy == .always or data.len > max_payload_size) {
                    return self.sendFragmented(channel_id, data, fragment_size);
                }
            }
            if (data.len > max_payload_size) return error.MessageTooLarge;

            const now_ns = self.cli.getCurrentTime();
            var send_state_ptr: ?*RelState = null;
            var seq: u16 = 0;

            switch (config.kind) {
                .Unreliable => {},
                .UnreliableLatest => {
                    if (comptime UlCount == 0) return error.InvalidChannel;
                    const index = Layout.latestIndex(channel_id) orelse return error.InvalidChannel;
                    const state = &self.ul_state[index];
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
            if (send_state_ptr) |send| {
                seq = send.push(data, now_ns) orelse unreachable;
            }

            outgoing.packet.Payload.header = .{
                .channel_id = channel_id,
                .seq = seq,
            };
            @memcpy(outgoing.packet.Payload.body[0..data.len], data);
            outgoing.packet.Payload.len = @intCast(data.len);
        }

        fn sendFragmented(self: *Self, channel_id: u8, data: []const u8, fragment_size: usize) !void {
            if (self.cli.state != .Connected) return error.InvalidState;

            if (data.len > std.math.maxInt(u16)) return error.MessageTooLarge;

            const fragment_count = if (data.len == 0) 1 else (data.len - 1) / fragment_size + 1;
            if (fragment_count > std.math.maxInt(u8)) return error.MessageTooLarge;
            const fragment_state = self.fragChannelPtr(channel_id) orelse return error.InvalidChannel;
            const reliable_send = self.reliableSendState(channel_id);
            // Admit the complete message before changing IDs, sequences, or queue lengths.
            if (reliable_send) |send| {
                if (!send.canPush(fragment_count)) return error.ReliableBufferFull;
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
                if (reliable_send) |send| {
                    seq = send.pushFragment(chunk, now_ns, fragment) orelse unreachable;
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

        pub fn connect(self: *Self) !void {
            try self.cli.connect();
        }

        pub fn connectSecure(self: *Self, token: ConnectTokenType) !void {
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

        fn sendAck(self: *Self, channel_id: u8, seq: u16) void {
            const out = self.cli.reservePayloadSlot() catch return;
            out.packet.Payload.header = .{ .channel_id = channel_id, .seq = seq, .is_ack = true };
        }

        fn handleAck(self: *Self, channel_id: u8, kind: channel_mod.ChannelKind, seq: u16) void {
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

        fn acceptLatest(self: *Self, channel_id: u8, seq: u16) bool {
            const recv = self.ulRecvState(channel_id) orelse return false;
            return recv.accept(seq);
        }

        fn handleReliableOrdered(
            self: *Self,
            channel_id: u8,
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

        fn handleFragmentBytes(self: *Self, channel_id: u8, frag: packet_mod.FragmentHeader, chunk: []const u8) void {
            const frag_sz = Layout.config(channel_id).fragment_size orelse return;
            const frag_ch = self.fragChannelPtr(channel_id) orelse return;
            const slot_idx = @as(usize, frag.msg_id) % FRAG_ASSEMBLY_SLOTS;
            const ab = &frag_ch.slots[slot_idx];

            if (ab.active and ab.msg_id != frag.msg_id) {
                ab.deinit(self.allocator);
            }

            if (!ab.active) {
                const data = self.allocator.alloc(u8, frag.total_size) catch return;
                ab.* = AssemblyBuf{
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

        fn acceptReliableUnordered(self: *Self, channel_id: u8, seq: u16) bool {
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

        fn reliableSendState(self: *Self, channel_id: u8) ?*RelState {
            return switch (Layout.kind(channel_id)) {
                .ReliableOrdered => self.orderedSendState(channel_id),
                .ReliableUnordered => self.unorderedSendState(channel_id),
                else => null,
            };
        }

        fn orderedSendState(self: *Self, channel_id: u8) ?*RelState {
            const idx = Layout.orderedIndex(channel_id) orelse return null;
            if (OrderedCount == 0) return null;
            return &self.ordered_state[idx].send;
        }

        fn unorderedSendState(self: *Self, channel_id: u8) ?*RelState {
            const idx = Layout.unorderedIndex(channel_id) orelse return null;
            if (UnorderedCount == 0) return null;
            return &self.unordered_state[idx].send;
        }

        fn orderedRecvState(self: *Self, channel_id: u8) ?*OrderedRecvState {
            const idx = Layout.orderedIndex(channel_id) orelse return null;
            if (OrderedCount == 0) return null;
            return &self.ordered_state[idx].recv;
        }

        fn unorderedRecvStatePtr(self: *Self, channel_id: u8) ?*UnorderedRecvState {
            const idx = Layout.unorderedIndex(channel_id) orelse return null;
            if (UnorderedCount == 0) return null;
            return &self.unordered_state[idx].recv;
        }

        fn ulRecvState(self: *Self, channel_id: u8) ?*channel_mod.UnreliableLatestState {
            const idx = Layout.latestIndex(channel_id) orelse return null;
            if (UlCount == 0) return null;
            return &self.ul_state[idx].recv;
        }

        fn fragChannelPtr(self: *Self, channel_id: u8) ?*FragChannelState {
            if (comptime Frag == 0) return null;
            const idx = Layout.fragIndex(channel_id) orelse return null;
            return &self.frag_state[idx];
        }

        fn resetChannelState(self: *Self) void {
            for (0..OrderedCount) |idx| {
                self.ordered_state[idx].recv.deinit(self.allocator);
            }
            for (0..Frag) |idx| {
                self.frag_state[idx].deinit(self.allocator);
            }
            self.ordered_state = @as([OrderedCount]OrderedChannelState, @splat(.{}));
            self.unordered_state = @as([UnorderedCount]UnorderedChannelState, @splat(.{}));
            self.ul_state = @as([UlCount]UlChannelState, @splat(.{}));
            self.rtt_state = .{};
            self.frag_state = @as([Frag]FragChannelState, @splat(.{}));
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
