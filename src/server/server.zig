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

    const pending_cap = opts.max_pending_clients orelse opts.max_clients * 2;
    const Conn = connection_mod.Connection(opts);
    const PendingConn = connection_mod.PendingConnection(opts);
    const Pkt = packet_mod.Packet(opts);
    const Pool = PayloadPool(opts.messages_queue_size, opts.max_payload_size);

    return struct {
        const Self = @This();

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
            cid: u64,
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
        addr_to_slot: std.AutoArrayHashMapUnmanaged(AddressKey, usize),
        recycled_slots: RingQueue(u64, pending_cap),
        slot_cursor: u64,
        pending: std.AutoArrayHashMapUnmanaged(AddressKey, PendingConn),

        outgoing: RingQueue(Outgoing, opts.outgoing_queue_size),
        events: RingQueue(Event, opts.events_queue_size),
        messages: RingQueue(RawMessageView, opts.messages_queue_size),
        payload_pool: Pool,

        pub fn init(allocator: std.mem.Allocator, config: ServerConfig) !Self {
            var io_threaded: std.Io.Threaded = .init(allocator, .{});
            errdefer io_threaded.deinit();
            const now = std.Io.Timestamp.now(io_threaded.io(), .awake);
            return .{
                .allocator = allocator,
                .protocol_id = config.protocol_id,
                .secure = config.secure,
                .secret_key = config.secret_key,
                .public_addresses = config.public_addresses,
                .recent_nonces = RecentNonces(opts.nonce_window).init(allocator),
                .challenge_key = config.challenge_key,
                .challenge_seq = 0,
                .config = config,
                .io_threaded = io_threaded,
                .start_time = now,
                .current_time = now,
                .clients = @as([opts.max_clients]?Conn, @splat(null)),
                .addr_to_slot = .empty,
                .recycled_slots = .{},
                .slot_cursor = 0,
                .pending = .empty,
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

        pub fn update(self: *Self, now: std.Io.Timestamp) void {
            self.current_time = now;
            const t = self.getCurrentTime();

            // Expire stale pending connections in-place (iterate backwards for safe swapRemove).
            var i: usize = self.pending.count();
            while (i > 0) {
                i -= 1;
                if (t > self.pending.values()[i].expires_at) {
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
            const slot: usize = @intCast(cid);
            const conn = &(self.clients[slot] orelse return error.UnknownClient);
            if (body.len > opts.max_payload_size) return error.PayloadTooLarge;

            conn.last_send = self.getCurrentTime();
            const out = self.outgoing.pushBackSlot() orelse return error.IoError;
            out.* = .{
                .addr = conn.addr,
                .packet = .{ .Payload = .{
                    .len = @intCast(body.len),
                    .body = undefined,
                } },
            };
            @memcpy(out.packet.Payload.body[0..body.len], body);
        }

        /// Reserve an outgoing payload slot for direct writes. Returns the
        /// writable body slice and the Outgoing entry. Caller must fill the
        /// body and set the length before the next flush.
        pub fn reservePayloadSlot(self: *Self, cid: u64) ServerError!*Outgoing {
            const slot: usize = @intCast(cid);
            const conn = &(self.clients[slot] orelse return error.UnknownClient);
            conn.last_send = self.getCurrentTime();
            const out = self.outgoing.pushBackSlot() orelse return error.IoError;
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

        pub fn handlePacket(self: *Self, addr: root.Address, buffer: []const u8) (ServerError || error{OutOfMemory})!void {
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
                    _ = try self.recent_nonces.insert(fields.client_nonce);

                    const cid: u64 = self.recycled_slots.popFront() orelse blk: {
                        if (self.slot_cursor >= opts.max_clients) return error.ServerFull;
                        const s = self.slot_cursor;
                        self.slot_cursor += 1;
                        break :blk s;
                    };

                    self.challenge_seq += 1;
                    const expires_at = self.getCurrentTime() + self.config.handshake_alive_ns;
                    const token = handshake.generateChallengeToken(
                        &self.challenge_key,
                        cid,
                        fields.client_nonce,
                        self.challenge_seq,
                        expires_at,
                    );

                    try self.pending.put(self.allocator, key, .{
                        .cid = cid,
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
                    })) return error.IoError;
                },
                .ConnectionResponse => |resp| {
                    const key = AddressKey.fromAddress(addr);
                    const pending = self.pending.get(key) orelse return error.UnknownClient;
                    if (self.getCurrentTime() > pending.expires_at) {
                        _ = self.pending.swapRemove(key);
                        return error.Expired;
                    }

                    const valid = handshake.verifyChallengeToken(
                        resp.token,
                        &self.challenge_key,
                        pending.cid,
                        pending.client_nonce,
                        resp.sequence,
                        pending.expires_at,
                    );
                    if (!valid) return error.InvalidPacket;

                    _ = self.pending.swapRemove(key);

                    const slot: usize = @intCast(pending.cid);
                    self.clients[slot] = .{
                        .cid = pending.cid,
                        .addr = addr,
                        .last_recv = self.getCurrentTime(),
                        .last_send = 0,
                        .user_data = pending.user_data,
                    };
                    try self.addr_to_slot.put(self.allocator, key, slot);

                    if (!self.outgoing.pushBack(.{
                        .addr = addr,
                        .packet = .ConnectionAccepted,
                    })) return error.IoError;

                    if (!self.events.pushBack(.{
                        .ClientConnected = .{
                            .cid = pending.cid,
                            .addr = addr,
                            .user_data = pending.user_data,
                        },
                    })) return error.IoError;
                },
                .Payload => |payload| {
                    const slot = self.addr_to_slot.get(AddressKey.fromAddress(addr)) orelse return error.UnknownClient;
                    const conn = &self.clients[slot].?;
                    conn.last_recv = self.getCurrentTime();
                    // Single copy: wire buffer -> pool.
                    const ref = self.payload_pool.allocCopy(buffer[packet_mod.PAYLOAD_HEADER_SIZE .. packet_mod.PAYLOAD_HEADER_SIZE + payload.len]) orelse return error.IoError;
                    if (!self.messages.pushBack(.{ .cid = conn.cid, .payload = ref })) {
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

            if (!self.recycled_slots.pushBack(conn.cid)) return error.IoError;
            errdefer _ = self.recycled_slots.popFront();

            if (!self.events.pushBack(.{
                .ClientDisconnected = .{
                    .cid = conn.cid,
                    .addr = conn.addr,
                },
            })) return error.IoError;

            self.clients[slot] = null;
            _ = self.addr_to_slot.swapRemove(AddressKey.fromAddress(conn.addr));
        }
    };
}
