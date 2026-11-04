const std = @import("std");
const zanoza = @import("zanoza");
const proto = @import("protocol");

// TransportServer(opts, void) — `void` selects the built-in UDP socket.
const Srv = zanoza.TransportServer(proto.net_opts, void);

const Slot = struct {
    cid: ?u64 = null, // zanoza client ID, stable for the lifetime of the connection
    x: f32 = proto.W / 2,
    y: f32 = proto.H / 2,
};

var slots: [4]Slot = @splat(.{});

fn sleepNs(ns: u64) !void {
    var io_threaded: std.Io.Threaded = .init(std.heap.page_allocator, .{});
    defer io_threaded.deinit();
    try std.Io.sleep(io_threaded.io(), .fromNanoseconds(@intCast(ns)), .awake);
}

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();

    const bind: zanoza.Address = .{ .ip4 = .unspecified(proto.SERVER_PORT) };

    // ServerConfig: protocol_id, handshake_timeout_ns, client_timeout_ns,
    //   public_addresses, secure, challenge_key, secret_key.
    // secure=false accepts plain (unauthenticated) connections.
    const config = zanoza.ServerConfig.init(
        proto.PROTOCOL_ID,
        10 * std.time.ns_per_s,
        30 * std.time.ns_per_s,
        &.{bind},
        false,
        @as([32]u8, @splat(0xAA)),
        null,
    );

    // Srv.init opens and binds the UDP socket, allocates per-client channel state,
    // and initialises the internal state machine.
    var srv = try Srv.init(gpa.allocator(), config, bind);
    defer srv.deinit();

    std.debug.print("Ball server listening on :{d}\n", .{proto.SERVER_PORT});

    while (true) {
        // tick() drives one iteration: recv all pending datagrams → state machine
        // → retransmit unACKed reliable messages → flush outgoing packets.
        srv.tick();

        // pollEvent yields ClientConnected / ClientDisconnected lifecycle events.
        // cid is a u64 that uniquely identifies this connection for its lifetime.
        while (srv.pollEvent()) |ev| {
            switch (ev) {
                .ClientConnected => |e| {
                    for (&slots, 0..) |*s, i| {
                        if (s.cid != null) continue;
                        s.* = .{ .cid = e.cid };
                        const id: u8 = @intCast(i);

                        // Tell the new client its assigned id.
                        var assign_buf: [proto.ASSIGN_SIZE]u8 = undefined;
                        proto.encodeAssign(&assign_buf, id);
                        srv.sendOnChannel(e.cid, proto.CH_SESSION, &assign_buf) catch {};

                        // Send all existing ball positions to the newly connected client
                        // so it can render peers that are already in the session.
                        for (slots, 0..) |other, j| {
                            if (other.cid == null or j == i) continue;
                            var ball_buf: [proto.BALL_SIZE]u8 = undefined;
                            proto.encodeBall(&ball_buf, @intCast(j), other.x, other.y);
                            srv.sendOnChannel(e.cid, proto.CH_BALL, &ball_buf) catch {};
                        }

                        std.debug.print("Connected cid={d} → slot {d}\n", .{ e.cid, i });
                        break;
                    }
                },
                .ClientDisconnected => |e| {
                    for (&slots, 0..) |*s, i| {
                        if (s.cid != e.cid) continue;
                        s.* = .{};

                        // Broadcast TAG_REMOVE so all peers hide this ball immediately.
                        var rm_buf: [proto.REMOVE_SIZE]u8 = undefined;
                        proto.encodeRemove(&rm_buf, @intCast(i));
                        for (slots) |other| {
                            if (other.cid) |cid| srv.sendOnChannel(cid, proto.CH_SESSION, &rm_buf) catch {};
                        }

                        std.debug.print("Disconnected cid={d} (slot {d})\n", .{ e.cid, i });
                        break;
                    }
                },
            }
        }

        // peekMessage returns a zero-copy view into the ring buffer.
        // msg.cid = sender, msg.channel_id = index into net_opts.channels.
        // consumeMessage advances the read pointer — call after processing.
        while (srv.peekMessage()) |msg| {
            const data = srv.messageData(msg);
            switch (msg.channel_id) {
                proto.CH_SESSION => {},
                proto.CH_BALL => {
                    if (proto.decodeBall(data)) |b| {
                        if (b.id < slots.len) {
                            slots[b.id].x = b.x;
                            slots[b.id].y = b.y;
                        }
                        // Relay position to every other connected client.
                        // The data slice is valid until consumeMessage(); sendOnChannel copies it.
                        for (slots) |other| {
                            if (other.cid) |cid| {
                                if (cid != msg.cid) srv.sendOnChannel(cid, proto.CH_BALL, data) catch {};
                            }
                        }
                    }
                },
                proto.CH_CHAT => {
                    // Relay chat to everyone including the sender (acts as echo confirmation).
                    for (slots) |other| {
                        if (other.cid) |cid| srv.sendOnChannel(cid, proto.CH_CHAT, data) catch {};
                    }
                },
                else => {},
            }
            srv.consumeMessage();
        }

        try sleepNs(16 * std.time.ns_per_ms);
    }
}
