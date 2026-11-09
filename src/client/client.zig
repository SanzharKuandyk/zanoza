const std = @import("std");
const root = @import("../root.zig");
const validation = @import("../validation/root.zig");
const packet_mod = @import("../packet.zig");
const Options = root.Options;
const RingQueue = @import("../ring_buffer.zig").RingQueue;
const PayloadPool = @import("../payload_pool.zig").PayloadPool;
const ClientError = @import("error.zig").ClientError;
const ClientConfig = @import("config.zig").ClientConfig;
const DefaultConnectToken = @import("../handshake.zig").DefaultConnectToken;

pub fn Client(comptime opts: Options) type {
    comptime validation.options.validate(opts);

    const Pkt = packet_mod.Packet(opts);
    const Pool = PayloadPool(opts.messages_queue_size, opts.max_payload_size);
    const ConnectTokenType = if (opts.ConnectToken == void)
        DefaultConnectToken(opts.user_data_size, opts.max_token_addresses)
    else
        opts.ConnectToken;

    return struct {
        const Self = @This();
        pub const Error = ClientError;

        pub const PayloadPool = Pool;

        pub const State = enum { Disconnected, SendingRequest, SendingResponse, Connected };

        pub const Event = enum { Connected, Disconnected };

        pub const RawMessageView = struct {
            header: packet_mod.PayloadHeader,
            payload: Pool.Ref,
        };

        pub const Outgoing = struct {
            addr: root.Address,
            packet: Pkt,
        };

        state: State,
        config: ClientConfig,

        client_nonce: u64,
        secure_token: ?ConnectTokenType,
        pending_response: ?packet_mod.ConnectionResponse,
        connect_started_at: u64,
        last_handshake_send: u64,
        has_handshake_send: bool,
        last_recv: u64,
        io_threaded: std.Io.Threaded,

        start_time: std.Io.Timestamp,
        current_time: std.Io.Timestamp,

        outgoing: RingQueue(Outgoing, opts.outgoing_queue_size),
        events: RingQueue(Event, opts.events_queue_size),
        messages: RingQueue(RawMessageView, opts.messages_queue_size),
        payload_pool: Pool,

        pub fn init(config: ClientConfig) ClientError!Self {
            var io_threaded: std.Io.Threaded = .init(std.heap.page_allocator, .{});
            errdefer io_threaded.deinit();
            const now = std.Io.Timestamp.now(io_threaded.io(), .awake);
            return .{
                .state = .Disconnected,
                .config = config,
                .client_nonce = 0,
                .secure_token = null,
                .pending_response = null,
                .connect_started_at = 0,
                .last_handshake_send = 0,
                .has_handshake_send = false,
                .last_recv = 0,
                .io_threaded = io_threaded,
                .start_time = now,
                .current_time = now,
                .outgoing = .{},
                .events = .{},
                .messages = .{},
                .payload_pool = Pool.init(),
            };
        }

        pub fn getCurrentTime(self: *const Self) u64 {
            return @intCast(self.start_time.durationTo(self.current_time).toNanoseconds());
        }

        pub fn update(self: *Self, now: std.Io.Timestamp) void {
            self.current_time = now;
            const t = self.getCurrentTime();
            switch (self.state) {
                .SendingRequest, .SendingResponse => self.updateHandshake(t),
                .Connected => {
                    if (t -| self.last_recv > self.config.timeout_ns) {
                        self.state = .Disconnected;
                        self.clearHandshakeState();
                        _ = self.events.pushBack(.Disconnected);
                    }
                },
                .Disconnected => {},
            }
        }

        pub fn updateNow(self: *Self) void {
            self.update(std.Io.Timestamp.now(self.io_threaded.io(), .awake));
        }

        pub fn connect(self: *Self) ClientError!void {
            if (self.state != .Disconnected) return error.InvalidState;
            self.io_threaded.io().random(std.mem.asBytes(&self.client_nonce));
            self.secure_token = null;
            self.pending_response = null;
            self.connect_started_at = self.getCurrentTime();
            self.last_handshake_send = 0;
            self.state = .SendingRequest;
            self.queueConnectionRequest() catch |err| {
                self.state = .Disconnected;
                self.clearHandshakeState();
                return err;
            };
        }

        pub fn connectSecure(self: *Self, token: ConnectTokenType) ClientError!void {
            if (self.state != .Disconnected) return error.InvalidState;
            self.io_threaded.io().random(std.mem.asBytes(&self.client_nonce));
            self.secure_token = token;
            self.pending_response = null;
            self.connect_started_at = self.getCurrentTime();
            self.last_handshake_send = 0;
            self.state = .SendingRequest;
            self.queueConnectionRequest() catch |err| {
                self.state = .Disconnected;
                self.clearHandshakeState();
                return err;
            };
        }

        pub fn handlePacket(self: *Self, buffer: []const u8) ClientError!void {
            const pkt = packet_mod.deserialize(opts, buffer) catch return error.InvalidPacket;

            switch (pkt) {
                .Challenge => |challenge| {
                    switch (self.state) {
                        .SendingRequest, .SendingResponse => {},
                        else => return error.InvalidPacket,
                    }
                    self.pending_response = .{
                        .sequence = challenge.sequence,
                        .token = challenge.token,
                    };
                    self.state = .SendingResponse;
                    try self.queueConnectionResponse();
                    self.last_recv = self.getCurrentTime();
                },
                .ConnectionAccepted => {
                    if (self.state != .SendingResponse) return error.InvalidPacket;
                    self.state = .Connected;
                    self.clearHandshakeState();
                    self.last_recv = self.getCurrentTime();
                    _ = self.events.pushBack(.Connected);
                },
                .Payload => |payload| {
                    if (self.state != .Connected) return error.InvalidPacket;
                    self.last_recv = self.getCurrentTime();
                    // Single copy: wire buffer -> pool.
                    const ref = self.payload_pool.allocCopy(buffer[payload.header.wireSize()..][0..payload.len]) orelse return error.IoError;
                    if (!self.messages.pushBack(.{ .header = payload.header, .payload = ref })) {
                        self.payload_pool.release(ref);
                        return error.IoError;
                    }
                },
                .Disconnect => {
                    if (self.state != .Connected) return error.InvalidPacket;
                    self.state = .Disconnected;
                    self.clearHandshakeState();
                    _ = self.events.pushBack(.Disconnected);
                },
                .ConnectionRequest, .ConnectionResponse => return error.InvalidPacket,
            }
        }

        pub fn sendPayload(self: *Self, body: []const u8) ClientError!void {
            if (self.state != .Connected) return error.InvalidState;
            if (body.len > opts.max_payload_size) return error.PayloadTooLarge;

            const out = self.outgoing.pushBackSlot() orelse return error.IoError;
            out.* = .{
                .addr = self.config.server_addr,
                .packet = .{ .Payload = .{
                    .len = @intCast(body.len),
                    .body = undefined,
                } },
            };
            @memcpy(out.packet.Payload.body[0..body.len], body);
        }

        /// Reserve an outgoing payload slot for direct writes.
        pub fn reservePayloadSlot(self: *Self) ClientError!*Outgoing {
            if (self.state != .Connected) return error.InvalidState;
            const out = self.outgoing.pushBackSlot() orelse return error.IoError;
            out.* = .{
                .addr = self.config.server_addr,
                .packet = .{ .Payload = .{
                    .len = 0,
                    .body = undefined,
                } },
            };
            return out;
        }

        pub fn disconnect(self: *Self) void {
            if (self.state != .Connected) return;
            self.state = .Disconnected;
            self.clearHandshakeState();
            _ = self.outgoing.pushBack(.{
                .addr = self.config.server_addr,
                .packet = .Disconnect,
            });
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

        pub fn deinit(self: *Self) void {
            self.io_threaded.deinit();
        }

        fn updateHandshake(self: *Self, now: u64) void {
            if (now -| self.connect_started_at > self.config.connect_timeout_ns) {
                self.state = .Disconnected;
                self.clearHandshakeState();
                _ = self.events.pushBack(.Disconnected);
                return;
            }
            if (self.has_handshake_send and now -| self.last_handshake_send < self.config.connect_retry_ns)
                return;

            switch (self.state) {
                .SendingRequest => self.queueConnectionRequest() catch {},
                .SendingResponse => self.queueConnectionResponse() catch {},
                else => {},
            }
        }

        fn queueConnectionRequest(self: *Self) ClientError!void {
            const req: packet_mod.ConnectionRequest(opts) = if (self.secure_token) |token|
                .{ .Secure = .{
                    .protocol_id = self.config.protocol_id,
                    .client_nonce = self.client_nonce,
                    .token = token,
                } }
            else
                .{ .Plain = .{
                    .protocol_id = self.config.protocol_id,
                    .client_nonce = self.client_nonce,
                } };

            if (!self.outgoing.pushBack(.{
                .addr = self.config.server_addr,
                .packet = .{ .ConnectionRequest = req },
            })) return error.IoError;

            self.last_handshake_send = self.getCurrentTime();
            self.has_handshake_send = true;
        }

        fn queueConnectionResponse(self: *Self) ClientError!void {
            const response = self.pending_response orelse return error.InvalidState;
            if (!self.outgoing.pushBack(.{
                .addr = self.config.server_addr,
                .packet = .{ .ConnectionResponse = response },
            })) return error.IoError;

            self.last_handshake_send = self.getCurrentTime();
            self.has_handshake_send = true;
        }

        fn clearHandshakeState(self: *Self) void {
            self.secure_token = null;
            self.pending_response = null;
            self.connect_started_at = 0;
            self.last_handshake_send = 0;
            self.has_handshake_send = false;
        }
    };
}
