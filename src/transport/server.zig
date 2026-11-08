const std = @import("std");
const root = @import("../root.zig");
const validation = @import("../validation/root.zig");
const packet_mod = @import("../packet.zig");
const server_mod = @import("../server/server.zig");
const channel_mod = @import("../channel.zig");
const state_mod = @import("../channel_state.zig");
const socket_mod = @import("socket.zig");
const RingQueue = @import("../ring_buffer.zig").RingQueue;
const UdpSocket = @import("udp.zig").UdpSocket;

pub fn TransportServer(comptime opts: root.Options, comptime SocketType: type) type {
    const Socket = if (SocketType == void) UdpSocket else SocketType;

    comptime {
        validation.options.validate(opts);
        if (SocketType != void) validation.socket.validate(Socket);
    }

    const Srv = server_mod.Server(opts);
    const PayloadHeader = packet_mod.PayloadHeader;
    const max_packet_size = packet_mod.maxPacketSize(opts);
    const max_user_data = opts.max_payload_size;
    const PoolRef = Srv.PayloadPool.Ref;

    const Layout = channel_mod.ChannelLayout(opts.channels);
    const channel_count = Layout.channel_count;

    const States = state_mod.States(opts.channels, opts.max_payload_size, .lazy);
    const initial_states = state_mod.initStates(opts.channels, opts.max_payload_size, .lazy);

    return struct {
        const Self = @This();

        fn ChannelState(comptime channel_id: u8) type {
            return state_mod.ChannelState(opts.channels[channel_id], opts.max_payload_size, .lazy);
        }

        pub const max_message_size = max_user_data;

        pub const Event = union(enum) {
            ClientConnected: struct {
                cid: u64,
                addr: root.Address,
                user_data: ?[opts.user_data_size]u8,
            },
            ClientDisconnected: struct {
                cid: u64,
                addr: root.Address,
            },
        };

        pub const Message = struct {
            cid: u64,
            channel_id: u8,
            data: [max_user_data]u8,
            len: usize,
        };

        /// Payload is either a reference into the SM pool (non-fragmented) or a
        /// heap-allocated buffer (assembled from fragments).
        pub const MessagePayload = union(enum) {
            pooled: PoolRef,
            assembled: []u8,
        };

        pub const MessageView = struct {
            cid: u64,
            channel_id: u8,
            payload: MessagePayload,
        };

        srv: Srv,
        socket: Socket,
        channel_states: *[opts.max_clients]States,
        rtt_state: *[opts.max_clients]channel_mod.RttState,
        allocator: std.mem.Allocator,
        events: RingQueue(Event, opts.events_queue_size),
        messages: RingQueue(MessageView, opts.messages_queue_size),

        pub fn init(allocator: std.mem.Allocator, config: root.ServerConfig, bind_addr: root.Address) !Self {
            const socket = try Socket.open(bind_addr);
            errdefer {
                var s = socket;
                s.close();
            }
            return initWithSocket(allocator, config, socket);
        }

        pub fn initWithSocket(allocator: std.mem.Allocator, config: root.ServerConfig, socket: Socket) !Self {
            const channel_states = try allocator.create([opts.max_clients]States);
            errdefer allocator.destroy(channel_states);
            for (channel_states) |*states| states.* = initial_states;

            const rtt_state = try allocator.create([opts.max_clients]channel_mod.RttState);
            errdefer allocator.destroy(rtt_state);
            rtt_state.* = @splat(.{});
            const srv = try Srv.init(allocator, config);
            return .{
                .srv = srv,
                .socket = socket,
                .channel_states = channel_states,
                .rtt_state = rtt_state,
                .allocator = allocator,
                .events = .{},
                .messages = .{},
            };
        }

        pub fn deinit(self: *Self) void {
            self.clearMessages();
            for (0..opts.max_clients) |slot| self.resetClientChannelState(slot);
            self.srv.deinit();
            self.socket.close();
            self.allocator.destroy(self.channel_states);
            self.allocator.destroy(self.rtt_state);
        }

        pub fn tick(self: *Self) void {
            self.srv.updateNow();
            const now_ns = self.srv.getCurrentTime();

            var buf: [max_packet_size]u8 = undefined;
            while (self.socket.recvfrom(&buf)) |result| {
                self.srv.handlePacket(result.addr, buf[0..result.len]) catch {};
            }

            while (self.srv.peekEvent()) |ev| {
                switch (ev.*) {
                    .ClientConnected => |e| {
                        self.resetClientChannelState(@intCast(e.cid));
                        _ = self.events.pushBack(.{ .ClientConnected = .{
                            .cid = e.cid,
                            .addr = e.addr,
                            .user_data = e.user_data,
                        } });
                    },
                    .ClientDisconnected => |e| {
                        self.resetClientChannelState(@intCast(e.cid));
                        _ = self.events.pushBack(.{ .ClientDisconnected = .{
                            .cid = e.cid,
                            .addr = e.addr,
                        } });
                    },
                }
                self.srv.consumeEvent();
            }

            // Drain SM message queue (payloads from wire -> pool).
            while (self.srv.peekMessage()) |raw| {
                const ref = raw.payload;
                const header = raw.header;
                const cid = raw.cid;
                self.srv.consumeMessage();
                self.handleIncoming(cid, header, ref);
            }

            self.retransmitReliable(now_ns);
            self.flushOutgoing();
        }

        fn flushOutgoing(self: *Self) void {
            var buf: [max_packet_size]u8 = undefined;
            while (self.srv.peekOutgoing()) |out| {
                const len = packet_mod.serialize(opts, out.packet, buf[0..]) catch {
                    self.srv.consumeOutgoing();
                    continue;
                };
                self.socket.sendto(out.addr, buf[0..len]);
                self.srv.consumeOutgoing();
            }
        }

        fn handleIncoming(self: *Self, cid: u64, hdr: PayloadHeader, ref: PoolRef) void {
            inline for (opts.channels, 0..) |_, index| {
                if (hdr.channel_id == index) return self.handleChannelIncoming(cid, @intCast(index), hdr, ref);
            }
            self.srv.releasePayload(ref);
        }

        fn handleChannelIncoming(self: *Self, cid: u64, comptime channel_id: u8, hdr: PayloadHeader, ref: PoolRef) void {
            const body = self.srv.payloadData(ref);
            const slot: usize = @intCast(cid);
            const ch_idx = channel_id;
            if (ch_idx >= channel_count) {
                self.srv.releasePayload(ref);
                return;
            }
            const kind = comptime Layout.kind(ch_idx);
            if (hdr.is_ack) {
                self.handleAck(slot, ch_idx, kind, hdr.seq);
                self.srv.releasePayload(ref);
                return;
            }

            switch (kind) {
                .Unreliable => {},
                .UnreliableLatest => {
                    if (!self.acceptLatest(slot, ch_idx, hdr.seq)) {
                        self.srv.releasePayload(ref);
                        return;
                    }
                },
                .ReliableOrdered => {
                    self.handleReliableOrdered(cid, slot, ch_idx, hdr.seq, hdr.fragment, ref);
                    return;
                },
                .ReliableUnordered => {
                    if (!self.acceptReliableUnordered(slot, cid, ch_idx, hdr.seq)) {
                        self.srv.releasePayload(ref);
                        return;
                    }
                },
            }

            if (hdr.fragment) |fragment| {
                self.handleFragmentBytes(cid, slot, ch_idx, fragment, body);
                self.srv.releasePayload(ref);
            } else {
                self.enqueueMessage(cid, ch_idx, .{ .pooled = ref });
            }
        }

        fn retransmitReliable(self: *Self, now_ns: u64) void {
            for (self.srv.addr_to_slot.values()) |slot| {
                const rto = self.rtt_state[slot].rto(opts.reliable_resend_ns);
                inline for (opts.channels, 0..) |config, channel_id| {
                    switch (config.kind) {
                        .ReliableOrdered, .ReliableUnordered => {
                            if (self.channel_states[slot][channel_id].send) |send| {
                                self.retransmitState(send, @intCast(slot), @intCast(channel_id), now_ns, rto);
                            }
                        },
                        else => {},
                    }
                }
            }
        }

        fn retransmitState(self: *Self, rel: anytype, cid: u64, comptime channel_id: u8, now_ns: u64, rto: u64) void {
            for (&rel.entries) |*e| {
                if (!e.active) continue;
                if (now_ns -| e.sent_at < rto) continue;

                const out = self.srv.reservePayloadSlot(cid) catch continue;
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

        pub fn sendOnChannel(self: *Self, cid: u64, channel_id: u8, data: []const u8) !void {
            inline for (opts.channels, 0..) |_, index| {
                if (channel_id == index) return self.sendOnConfiguredChannel(cid, @intCast(index), data);
            }
            return error.InvalidChannel;
        }

        fn sendOnConfiguredChannel(self: *Self, cid: u64, comptime channel_id: u8, data: []const u8) !void {
            if (channel_id >= channel_count) return error.InvalidChannel;
            const slot = std.math.cast(usize, cid) orelse return error.UnknownClient;
            if (slot >= opts.max_clients or self.srv.clients[slot] == null) return error.UnknownClient;

            const config = comptime Layout.config(channel_id);
            if (config.frag_config) |cfg| {
                if (cfg.policy == .always or data.len > max_user_data) {
                    return self.sendFragmented(cid, channel_id, data, cfg.fragment_size);
                }
            }
            if (data.len > max_user_data) return error.MessageTooLarge;

            const now_ns = self.srv.getCurrentTime();
            var send_state_ptr: ?*ChannelState(channel_id).Send = null;
            var seq: u16 = 0;

            switch (config.kind) {
                .Unreliable => {},
                .UnreliableLatest => {
                    const state = &self.channel_states[slot][channel_id];
                    state.send_seq +%= 1;
                    seq = state.send_seq;
                },
                .ReliableOrdered => {
                    const send_ptr = self.orderedSendPtr(slot, channel_id) orelse return error.InvalidChannel;
                    const send = try self.getOrCreateReliableState(channel_id, send_ptr);
                    if (send.isNextNotEmpty()) return error.ReliableBufferFull;
                    send_state_ptr = send;
                },
                .ReliableUnordered => {
                    const send_ptr = self.unorderedSendPtr(slot, channel_id) orelse return error.InvalidChannel;
                    const send = try self.getOrCreateReliableState(channel_id, send_ptr);
                    if (send.isNextNotEmpty()) return error.ReliableBufferFull;
                    send_state_ptr = send;
                },
            }

            // Commit the reliable entry only after reserving the outgoing slot.
            const outgoing = try self.srv.reservePayloadSlot(cid);
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

        fn sendFragmented(self: *Self, cid: u64, comptime channel_id: u8, data: []const u8, fragment_size: usize) !void {
            const slot = std.math.cast(usize, cid) orelse return error.UnknownClient;
            if (slot >= opts.max_clients or self.srv.clients[slot] == null) return error.UnknownClient;
            if (data.len > std.math.maxInt(u16)) return error.MessageTooLarge;
            const fragment_count = if (data.len == 0) 1 else (data.len - 1) / fragment_size + 1;
            if (fragment_count > std.math.maxInt(u8)) return error.MessageTooLarge;
            const fragment_state = self.fragChannelPtr(slot, channel_id) orelse return error.InvalidChannel;
            const reliable_send = try self.reliableSendState(slot, channel_id);
            // Admit the complete message before changing IDs, sequences, or queue lengths.
            if (comptime opts.channels[channel_id].kind == .ReliableOrdered or opts.channels[channel_id].kind == .ReliableUnordered) {
                if (reliable_send) |send| {
                    if (!send.canPush(fragment_count)) return error.ReliableBufferFull;
                }
            }
            if (fragment_count > opts.outgoing_queue_size - self.srv.outgoing.len) return error.IoError;
            fragment_state.next_msg_id +%= 1;
            const now_ns = self.srv.getCurrentTime();
            for (0..fragment_count) |index| {
                const offset = index * fragment_size;
                const chunk = data[offset..@min(offset + fragment_size, data.len)];
                const fragment: packet_mod.FragmentHeader = .{
                    .msg_id = fragment_state.next_msg_id,
                    .frag_count = @intCast(fragment_count),
                    .frag_index = @intCast(index),
                    .total_size = @intCast(data.len),
                };
                const outgoing = self.srv.reservePayloadSlot(cid) catch unreachable;
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

        pub fn pollEvent(self: *Self) ?Event {
            // Copy-out convenience API.
            return self.events.popFront();
        }

        pub fn pollMessage(self: *Self) ?Message {
            // Copy-out convenience API built on top of the zero-copy message view.
            // Note: assembled (fragmented) messages are truncated to max_user_data bytes.
            // Use peekMessage/messageData/consumeMessage for full access to large messages.
            const msg = self.peekMessage() orelse return null;
            const data = self.messageData(msg);
            const copy_len = @min(data.len, max_user_data);
            var owned: Message = .{
                .cid = msg.cid,
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
                .pooled => |ref| self.srv.payloadData(ref),
                .assembled => |data| data,
            };
        }

        pub fn consumeMessage(self: *Self) void {
            const msg = self.messages.peekFront() orelse return;
            switch (msg.payload) {
                .pooled => |ref| self.srv.releasePayload(ref),
                .assembled => |data| self.allocator.free(data),
            }
            self.messages.advance();
        }

        pub fn getStateMachine(self: *Self) *Srv {
            return &self.srv;
        }

        fn sendAck(self: *Self, cid: u64, comptime channel_id: u8, seq: u16) void {
            const out = self.srv.reservePayloadSlot(cid) catch return;
            out.packet.Payload.header = .{ .channel_id = channel_id, .seq = seq, .is_ack = true };
        }

        fn handleAck(self: *Self, slot: usize, comptime channel_id: u8, comptime kind: channel_mod.ChannelKind, seq: u16) void {
            const now_ns = self.srv.getCurrentTime();
            const sent_at: ?u64 = switch (kind) {
                .ReliableOrdered => blk: {
                    const send_ptr = self.orderedSendPtr(slot, channel_id) orelse break :blk null;
                    break :blk if (send_ptr.*) |rel| rel.ack(seq) else null;
                },
                .ReliableUnordered => blk: {
                    const send_ptr = self.unorderedSendPtr(slot, channel_id) orelse break :blk null;
                    break :blk if (send_ptr.*) |rel| rel.ack(seq) else null;
                },
                else => null,
            };

            if (sent_at) |t| {
                self.rtt_state[slot].update(now_ns -| t);
            }
        }

        fn acceptLatest(self: *Self, slot: usize, comptime channel_id: u8, seq: u16) bool {
            const recv = self.ulRecvState(slot, channel_id) orelse return false;
            return recv.accept(seq);
        }

        fn handleReliableOrdered(
            self: *Self,
            cid: u64,
            slot: usize,
            comptime channel_id: u8,
            seq: u16,
            fragment: ?packet_mod.FragmentHeader,
            ref: PoolRef,
        ) void {
            const body = self.srv.payloadData(ref);
            const recv = self.orderedRecvState(slot, channel_id) orelse {
                self.srv.releasePayload(ref);
                return;
            };
            const action = recv.receiveFragment(self.allocator, seq, body, fragment) catch {
                self.srv.releasePayload(ref);
                return;
            };
            if (action == .drop) {
                self.srv.releasePayload(ref);
                return;
            }
            self.sendAck(cid, channel_id, seq);
            if (action == .ack_only) {
                self.srv.releasePayload(ref);
                return;
            }
            if (fragment) |frag| {
                self.handleFragmentBytes(cid, slot, channel_id, frag, body);
                self.srv.releasePayload(ref);
            } else {
                self.enqueueMessage(cid, channel_id, .{ .pooled = ref });
            }
            while (recv.peekReady()) |ready_body| {
                if (recv.readyFragment()) |frag| {
                    self.handleFragmentBytes(cid, slot, channel_id, frag, ready_body);
                } else {
                    const future_ref = self.srv.payload_pool.allocCopy(ready_body) orelse break;
                    self.enqueueMessage(cid, channel_id, .{ .pooled = future_ref });
                }
                recv.consumeReady();
            }
        }

        fn handleFragmentBytes(self: *Self, cid: u64, slot: usize, comptime channel_id: u8, frag: packet_mod.FragmentHeader, chunk: []const u8) void {
            if (comptime opts.channels[channel_id].frag_config == null) return;
            const frag_sz = if (Layout.config(channel_id).frag_config) |cfg| cfg.fragment_size else return;
            const frag_ch = self.fragChannelPtr(slot, channel_id) orelse return;
            const slot_idx = @as(usize, frag.msg_id) % frag_ch.slots.len;
            const ab = &frag_ch.slots[slot_idx];

            // If this slot belongs to a different message, evict and reuse it.
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

            // Write chunk at the correct byte offset within the assembled buffer.
            const chunk_offset = @as(usize, frag.frag_index) * frag_sz;
            const assembled_data = ab.data.?;
            if (chunk_offset < assembled_data.len) {
                const copy_len = @min(chunk.len, assembled_data.len - chunk_offset);
                @memcpy(assembled_data[chunk_offset .. chunk_offset + copy_len], chunk[0..copy_len]);
            }

            if (ab.frags_received == ab.frag_count) {
                // Transfer ownership of the assembled buffer to the message queue.
                const assembled = ab.data.?;
                ab.* = .{}; // data field defaults to null; no double-free
                self.enqueueMessage(cid, channel_id, .{ .assembled = assembled });
            }
        }

        fn acceptReliableUnordered(self: *Self, slot: usize, cid: u64, comptime channel_id: u8, seq: u16) bool {
            const recv = self.unorderedRecvStatePtr(slot, channel_id) orelse return false;
            const action = recv.classify(seq);
            if (action != .stale) self.sendAck(cid, channel_id, seq);
            return action == .deliver;
        }

        fn enqueueMessage(self: *Self, cid: u64, channel_id: u8, payload: MessagePayload) void {
            if (!self.messages.pushBack(.{
                .cid = cid,
                .channel_id = channel_id,
                .payload = payload,
            })) {
                switch (payload) {
                    .pooled => |ref| self.srv.releasePayload(ref),
                    .assembled => |data| self.allocator.free(data),
                }
            }
        }

        fn reliableSendState(self: *Self, slot: usize, comptime channel_id: u8) !?*ChannelState(channel_id).Send {
            const state = switch (opts.channels[channel_id].kind) {
                .ReliableOrdered => self.orderedSendPtr(slot, channel_id),
                .ReliableUnordered => self.unorderedSendPtr(slot, channel_id),
                else => return null,
            } orelse return error.InvalidChannel;
            return try self.getOrCreateReliableState(channel_id, state);
        }

        fn orderedSendPtr(self: *Self, slot: usize, comptime channel_id: u8) ?*?*ChannelState(channel_id).Send {
            if (comptime opts.channels[channel_id].kind != .ReliableOrdered) return null;
            return &self.channel_states[slot][channel_id].send;
        }

        fn unorderedSendPtr(self: *Self, slot: usize, comptime channel_id: u8) ?*?*ChannelState(channel_id).Send {
            if (comptime opts.channels[channel_id].kind != .ReliableUnordered) return null;
            return &self.channel_states[slot][channel_id].send;
        }

        fn orderedRecvState(self: *Self, slot: usize, comptime channel_id: u8) ?*ChannelState(channel_id).Recv {
            if (comptime opts.channels[channel_id].kind != .ReliableOrdered) return null;
            return &self.channel_states[slot][channel_id].recv;
        }

        fn unorderedRecvStatePtr(self: *Self, slot: usize, comptime channel_id: u8) ?*ChannelState(channel_id).Recv {
            if (comptime opts.channels[channel_id].kind != .ReliableUnordered) return null;
            return &self.channel_states[slot][channel_id].recv;
        }

        fn ulRecvState(self: *Self, slot: usize, comptime channel_id: u8) ?*channel_mod.UnreliableLatestState {
            if (comptime opts.channels[channel_id].kind != .UnreliableLatest) return null;
            return &self.channel_states[slot][channel_id].recv;
        }

        fn fragChannelPtr(self: *Self, slot: usize, comptime channel_id: u8) ?*ChannelState(channel_id).Fragments {
            if (comptime opts.channels[channel_id].frag_config == null) return null;
            return &self.channel_states[slot][channel_id].fragments;
        }

        fn getOrCreateReliableState(self: *Self, comptime channel_id: u8, send_ptr: *?*ChannelState(channel_id).Send) !*ChannelState(channel_id).Send {
            if (send_ptr.*) |rel| return rel;

            // Unused reliable channels stay cheap until first use.
            const rel = try self.allocator.create(ChannelState(channel_id).Send);
            rel.* = .{};
            send_ptr.* = rel;
            return rel;
        }

        fn resetClientChannelState(self: *Self, slot: usize) void {
            inline for (opts.channels, 0..) |_, index| {
                self.channel_states[slot][index].deinit(self.allocator);
            }
            self.rtt_state[slot] = .{};
        }

        fn clearMessages(self: *Self) void {
            while (self.messages.peekFront()) |msg| {
                switch (msg.payload) {
                    .pooled => |ref| self.srv.releasePayload(ref),
                    .assembled => |data| self.allocator.free(data),
                }
                self.messages.advance();
            }
        }
    };
}
