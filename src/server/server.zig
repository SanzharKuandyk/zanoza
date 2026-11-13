const std = @import("std");
const root = @import("../root.zig");
const validation = @import("../validation/root.zig");
const connection_mod = @import("connection.zig");
const packet_mod = @import("../packet.zig");
const handshake = @import("../handshake.zig");
const Options = root.Options;
const AddressKey = @import("../addr.zig").AddressKey;
const RingQueue = @import("../ring_buffer.zig").RingQueue;
const RecentNonces = @import("../nonce.zig").RecentNonces;
const PayloadPool = @import("../payload_pool.zig").PayloadPool;
const ServerError = @import("error.zig").ServerError;
const ServerConfig = @import("config.zig").ServerConfig;

const CHALLENGE_KEY_SIZE = root.CHALLENGE_KEY_SIZE;
const SECRET_KEY_SIZE = root.SECRET_KEY_SIZE;

pub fn Server(comptime opts: Options) type {
    comptime {
        validation.options.validate(opts);
        if (opts.ConnectToken != void)
            validation.connect_token.validate(opts.ConnectToken, opts.user_data_size);
    }

    // Each pending handshake holds one of the server's client slots.
    const pending_cap = @min(opts.max_pending_clients orelse opts.max_clients * 2, opts.max_clients);
    const Conn = connection_mod.Connection(opts);
    const PendingConn = connection_mod.PendingConnection(opts);
    const AddressToSlot = std.AutoArrayHashMapUnmanaged(AddressKey, usize);
    const PendingConnections = std.AutoArrayHashMapUnmanaged(AddressKey, PendingConn);
    const Pkt = packet_mod.Packet(opts);
    const PayloadHeader = packet_mod.PayloadHeader;
    const Pool = PayloadPool(opts.messages_queue_size, opts.max_payload_size);

    return struct {
        const Self = @This();
        pub const Error = ServerError;

        pub const PayloadPool = Pool;

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

        pub const RawMessageView = struct {
            header: PayloadHeader,
            cid: u64,
            slot: usize,
            connection_sequence: u64,
            payload: Pool.Ref,
        };

        pub const Outgoing = struct {
            addr: root.Address,
            packet: Pkt,
        };

        allocator: std.mem.Allocator,
        protocol_id: u32,
        secure: bool,
        secret_key: ?[SECRET_KEY_SIZE]u8,
        public_addresses: []const root.Address,

        recent_nonces: RecentNonces(opts.nonce_window),

        challenge_key: [CHALLENGE_KEY_SIZE]u8,
        challenge_seq: u64,

        config: ServerConfig,
        io_threaded: std.Io.Threaded,

        start_time: std.Io.Timestamp,
        current_time: std.Io.Timestamp,

        clients: [opts.max_clients]?Conn,
        addr_to_slot: AddressToSlot,
        free_slots: std.bit_set.Static(opts.max_clients),
        pending: PendingConnections,

        outgoing: RingQueue(Outgoing, opts.outgoing_queue_size),
        events: RingQueue(Event, opts.events_queue_size),
        messages: RingQueue(RawMessageView, opts.messages_queue_size),
        payload_pool: Pool,

        pub fn init(allocator: std.mem.Allocator, config: ServerConfig) ServerError!Self {
            var addr_to_slot: AddressToSlot = .empty;
            errdefer addr_to_slot.deinit(allocator);
            try addr_to_slot.ensureTotalCapacity(allocator, opts.max_clients);

            var pending: PendingConnections = .empty;
            errdefer pending.deinit(allocator);
            try pending.ensureTotalCapacity(allocator, pending_cap);

            var io_threaded: std.Io.Threaded = .init(allocator, .{});
            errdefer io_threaded.deinit();
            const now = std.Io.Timestamp.now(io_threaded.io(), .awake);
            return .{
                .allocator = allocator,
                .protocol_id = config.protocol_id,
                .secure = config.secure,
                .secret_key = config.secret_key,
                .public_addresses = config.public_addresses,
                .recent_nonces = try RecentNonces(opts.nonce_window).init(allocator),
                .challenge_key = config.challenge_key,
                .challenge_seq = 0,
                .config = config,
                .io_threaded = io_threaded,
                .start_time = now,
                .current_time = now,
                .clients = @as([opts.max_clients]?Conn, @splat(null)),
                .addr_to_slot = addr_to_slot,
                .free_slots = .full,
                .pending = pending,
                .outgoing = .{},
                .events = .{},
                .messages = .{},
                .payload_pool = Pool.init(),
            };
        }

        pub fn deinit(self: *Self) void {
            self.addr_to_slot.deinit(self.allocator);
            self.pending.deinit(self.allocator);
            self.recent_nonces.deinit();
            self.io_threaded.deinit();
        }

        pub fn getCurrentTime(self: *const Self) u64 {
            return @intCast(self.start_time.durationTo(self.current_time).toNanoseconds());
        }

        pub fn getClientSlot(self: *const Self, cid: u64) error{UnknownClient}!usize {
            const slot = std.math.cast(usize, cid) orelse return error.UnknownClient;
            if (slot >= opts.max_clients or self.clients[slot] == null) return error.UnknownClient;

            return slot;
        }

        pub fn update(self: *Self, now: std.Io.Timestamp) void {
            self.current_time = now;
            const t = self.getCurrentTime();

            // Expire stale pending connections in-place (iterate backwards for safe swapRemove).
            var i: usize = self.pending.count();
            while (i > 0) {
                i -= 1;
                if (t > self.pending.values()[i].expires_at) {
                    self.free_slots.set(self.pending.values()[i].slot);
                    self.pending.swapRemoveAt(i);
                }
            }

            // Iterate only connected clients via addr_to_slot (reverse for safe swapRemove).
            i = self.addr_to_slot.count();
            while (i > 0) {
                i -= 1;
                const slot = self.addr_to_slot.values()[i];
                const conn = self.clients[slot] orelse continue;
                if (t -| conn.last_recv <= self.config.client_timeout_ns) continue;

                self.disconnectClient(slot) catch {};
            }
        }

        pub fn updateNow(self: *Self) void {
            self.update(std.Io.Timestamp.now(self.io_threaded.io(), .awake));
        }

        pub fn sendPayload(self: *Self, cid: u64, body: []const u8) ServerError!void {
            const slot = try self.getClientSlot(cid);
            if (body.len > opts.max_payload_size) return error.PayloadTooLarge;

            const out = try self.reservePayloadForClient(&self.clients[slot].?);
            @memcpy(out.packet.Payload.body[0..body.len], body);
            out.packet.Payload.len = @intCast(body.len);
        }

        /// Queue a payload entry for direct writes. Fill its body and length before flushing.
        pub fn reservePayloadSlot(self: *Self, cid: u64) ServerError!*Outgoing {
            const slot = try self.getClientSlot(cid);
            return self.reservePayloadForClient(&self.clients[slot].?);
        }

        /// Slot-based form used by the transport after it has resolved the client ID.
        pub fn reservePayloadAt(self: *Self, slot: usize) error{ UnknownClient, IoError }!*Outgoing {
            if (slot >= self.clients.len) return error.UnknownClient;
            if (self.clients[slot]) |*conn| return self.reservePayloadForClient(conn);
            return error.UnknownClient;
        }

        fn reservePayloadForClient(self: *Self, conn: *Conn) error{IoError}!*Outgoing {
            const out = self.outgoing.pushBackSlot() orelse return error.IoError;
            conn.last_send = self.getCurrentTime();
            out.* = .{
                .addr = conn.addr,
                .packet = .{ .Payload = .{
                    .len = 0,
                    .body = undefined,
                } },
            };
            return out;
        }

        pub fn pollEvent(self: *Self) ?Event {
            // Copy-out convenience API.
            return self.events.popFront();
        }

        pub fn peekEvent(self: *const Self) ?*const Event {
            // Zero-copy view into the front event.
            return self.events.peekFront();
        }

        pub fn consumeEvent(self: *Self) void {
            // Drop the event returned by peekEvent().
            self.events.advance();
        }

        pub fn peekMessage(self: *const Self) ?*const RawMessageView {
            return self.messages.peekFront();
        }

        pub fn consumeMessage(self: *Self) void {
            // Advance ring only; does NOT release pool ref.
            self.messages.advance();
        }

        pub fn releasePayload(self: *Self, ref: Pool.Ref) void {
            self.payload_pool.release(ref);
        }

        pub fn payloadData(self: *const Self, ref: Pool.Ref) []const u8 {
            return self.payload_pool.slice(ref);
        }

        pub fn pollOutgoing(self: *Self) ?Outgoing {
            // Copy-out convenience API.
            return self.outgoing.popFront();
        }

        pub fn peekOutgoing(self: *const Self) ?*const Outgoing {
            // Zero-copy view into the front outgoing packet.
            return self.outgoing.peekFront();
        }

        pub fn consumeOutgoing(self: *Self) void {
            // Drop the packet returned by peekOutgoing().
            self.outgoing.advance();
        }

        pub fn handlePacket(self: *Self, addr: root.Address, buffer: []const u8) ServerError!void {
            const pkt = packet_mod.deserialize(opts, buffer) catch return error.InvalidPacket;

            switch (pkt) {
                .ConnectionRequest => |req| {
                    const key = AddressKey.fromAddress(addr);
                    if (self.pending.contains(key) or self.addr_to_slot.contains(key))
                        return error.InvalidPacket;

                    const Fields = struct {
                        protocol_id: u32,
                        client_nonce: u64,
                        user_data: ?[opts.user_data_size]u8,
                    };

                    const fields: Fields = switch (req) {
                        .Plain => |r| blk: {
                            if (self.secure) return error.InvalidPacket;
                            break :blk .{
                                .protocol_id = r.protocol_id,
                                .client_nonce = r.client_nonce,
                                .user_data = null,
                            };
                        },
                        .Secure => |r| blk: {
                            if (!self.secure or self.secret_key == null) return error.InvalidPacket;
                            if (!r.token.verify(self.getCurrentTime(), &self.secret_key.?)) return error.InvalidPacket;
                            if (!r.token.authorizeAddress(addr)) return error.InvalidPacket;
                            break :blk .{
                                .protocol_id = r.protocol_id,
                                .client_nonce = r.client_nonce,
                                .user_data = r.token.user_data,
                            };
                        },
                    };

                    if (self.protocol_id != fields.protocol_id) return error.InvalidProtocolId;
                    if (self.recent_nonces.contains(fields.client_nonce)) return error.InvalidPacket;
                    if (self.pending.count() >= pending_cap) return error.ServerFull;
                    const slot = self.free_slots.findFirstSet() orelse return error.ServerFull;
                    if (self.outgoing.isFull()) return error.IoError;
                    if (!self.recent_nonces.insert(fields.client_nonce)) return error.InvalidPacket;

                    self.free_slots.unset(slot);
                    self.challenge_seq +%= 1;
                    const expires_at = self.getCurrentTime() + self.config.handshake_alive_ns;
                    const token = handshake.generateChallengeToken(
                        &self.challenge_key,
                        @intCast(slot),
                        fields.client_nonce,
                        self.challenge_seq,
                        expires_at,
                    );

                    self.pending.putAssumeCapacityNoClobber(key, .{
                        .slot = slot,
                        .client_nonce = fields.client_nonce,
                        .sequence = self.challenge_seq,
                        .expires_at = expires_at,
                        .user_data = fields.user_data,
                    });

                    if (!self.outgoing.pushBack(.{
                        .addr = addr,
                        .packet = .{ .Challenge = .{
                            .sequence = self.challenge_seq,
                            .expires_at = expires_at,
                            .token = token,
                        } },
                    })) unreachable;
                },
                .ConnectionResponse => |resp| {
                    const key = AddressKey.fromAddress(addr);
                    const pending = self.pending.get(key) orelse return error.UnknownClient;
                    if (self.getCurrentTime() > pending.expires_at) {
                        self.free_slots.set(pending.slot);
                        _ = self.pending.swapRemove(key);
                        return error.Expired;
                    }

                    const valid = handshake.verifyChallengeToken(
                        resp.token,
                        &self.challenge_key,
                        @intCast(pending.slot),
                        pending.client_nonce,
                        resp.sequence,
                        pending.expires_at,
                    );
                    if (!valid) return error.InvalidPacket;

                    if (self.outgoing.isFull() or self.events.isFull()) return error.IoError;

                    _ = self.pending.swapRemove(key);

                    const slot = pending.slot;
                    const cid: u64 = @intCast(slot);
                    self.clients[slot] = .{
                        .cid = cid,
                        .sequence = pending.sequence,
                        .addr = addr,
                        .last_recv = self.getCurrentTime(),
                        .last_send = 0,
                        .user_data = pending.user_data,
                    };
                    self.addr_to_slot.putAssumeCapacityNoClobber(key, slot);

                    if (!self.outgoing.pushBack(.{
                        .addr = addr,
                        .packet = .ConnectionAccepted,
                    })) unreachable;

                    if (!self.events.pushBack(.{
                        .ClientConnected = .{
                            .cid = cid,
                            .addr = addr,
                            .user_data = pending.user_data,
                        },
                    })) unreachable;
                },
                .Payload => |payload| {
                    const slot = self.addr_to_slot.get(AddressKey.fromAddress(addr)) orelse return error.UnknownClient;
                    const conn = &self.clients[slot].?;
                    conn.last_recv = self.getCurrentTime();
                    // Single copy: wire buffer -> pool.
                    const ref = self.payload_pool.allocCopy(buffer[payload.header.wireSize()..][0..payload.len]) orelse return error.IoError;
                    if (!self.messages.pushBack(.{
                        .cid = conn.cid,
                        .slot = slot,
                        .connection_sequence = conn.sequence,
                        .header = payload.header,
                        .payload = ref,
                    })) {
                        self.payload_pool.release(ref);
                        return error.IoError;
                    }
                },
                .Disconnect => {
                    const key = AddressKey.fromAddress(addr);
                    const slot = self.addr_to_slot.get(key) orelse return error.UnknownClient;
                    try self.disconnectClient(slot);
                },
                .Challenge, .ConnectionAccepted => return error.InvalidPacket,
            }
        }

        fn disconnectClient(self: *Self, slot: usize) ServerError!void {
            const conn = self.clients[slot] orelse return error.UnknownClient;

            if (!self.events.pushBack(.{
                .ClientDisconnected = .{
                    .cid = conn.cid,
                    .addr = conn.addr,
                },
            })) return error.IoError;

            self.clients[slot] = null;
            _ = self.addr_to_slot.swapRemove(AddressKey.fromAddress(conn.addr));
            self.free_slots.set(slot);
        }
    };
}
