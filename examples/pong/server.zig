const std = @import("std");
const zanoza = @import("zanoza");
const proto = @import("protocol");

// TransportServer(opts, void) — the second argument is the socket type.
// `void` selects the built-in non-blocking UDP socket. A custom type satisfying
// the socket interface (open/close/recvfrom/sendto) can be passed instead.
const Srv = zanoza.TransportServer(proto.net_opts, void);

// zanoza has no server-side connection timeout for established clients.
// We track the last received input per slot and evict slots that go silent,
// which handles clients that are killed without sending a Disconnect packet.
const HEARTBEAT_TIMEOUT_MS: i64 = 10_000;

const PlayerSlot = struct {
    cid: ?u64 = null, // zanoza client ID assigned by the server state machine
    y: f32 = proto.SCREEN_H / 2 - proto.PADDLE_H / 2,
    dir: i8 = 0,
    last_heartbeat_ms: i64 = 0, // 0 = grace period (just connected, no input yet)
    ready: bool = false,
};

var players: [2]PlayerSlot = .{ .{}, .{} };
var ball_x: f32 = proto.SCREEN_W / 2;
var ball_y: f32 = proto.SCREEN_H / 2;
var vel_x: f32 = proto.BALL_SPEED;
var vel_y: f32 = proto.BALL_SPEED * 0.6;
var score: [2]u8 = .{ 0, 0 };
var game_over: bool = false;

fn sleepNs(ns: u64) !void {
    var io_threaded: std.Io.Threaded = .init(std.heap.page_allocator, .{});
    defer io_threaded.deinit();
    try std.Io.sleep(io_threaded.io(), .fromNanoseconds(@intCast(ns)), .awake);
}

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();
    var io_threaded: std.Io.Threaded = .init(alloc, .{});
    defer io_threaded.deinit();

    const bind: zanoza.Address = .{ .ip4 = .unspecified(proto.SERVER_PORT) };

    // ServerConfig.init(protocol_id, handshake_alive_ns, client_timeout_ns,
    //   public_addresses, secure, challenge_key, secret_key)
    // `secure = false` means plain (unauthenticated) connections are accepted.
    // `secret_key = null` because secure mode is off.
    const config = zanoza.ServerConfig.init(
        proto.PROTOCOL_ID,
        10 * std.time.ns_per_s,
        30 * std.time.ns_per_s,
        &.{bind},
        false,
        @as([32]u8, @splat(0xAA)),
        null,
    );

    // TransportServer.init opens and binds the UDP socket, allocates channel state
    // for all max_clients slots, and initialises the internal state machine.
    var srv = try Srv.init(alloc, config, bind);
    defer srv.deinit();

    std.debug.print("Pong server listening on :{d}\n", .{proto.SERVER_PORT});

    var prev_tick = std.Io.Timestamp.now(io_threaded.io(), .awake);
    var prev_both_connected = false;

    while (true) {
        const now = std.Io.Timestamp.now(io_threaded.io(), .awake);
        const dt_ns = prev_tick.durationTo(now).toNanoseconds();
        prev_tick = now;
        const dt: f32 = @as(f32, @floatFromInt(dt_ns)) / @as(f32, @floatFromInt(std.time.ns_per_s));
        const now_ms = now.toMilliseconds();

        // tick() drives one iteration: recv all pending datagrams → run state machine
        // → retransmit unACKed reliable messages → flush outgoing packets.
        srv.tick();

        for (&players, 0..) |*p, i| {
            if (p.cid == null or p.last_heartbeat_ms == 0) continue;
            if (now_ms - p.last_heartbeat_ms > HEARTBEAT_TIMEOUT_MS) {
                std.debug.print("Evicting silent slot {d} (cid={?d})\n", .{ i, p.cid });
                if (!game_over) notifyGameOver(&srv, @intCast(i ^ 1));
                p.* = .{};
                game_over = true;
            }
        }

        // pollEvent returns ClientConnected / ClientDisconnected lifecycle events.
        // cid is a u64 slot index stable for the lifetime of that connection.
        while (srv.pollEvent()) |ev| {
            switch (ev) {
                .ClientConnected => |e| {
                    std.debug.print("Client connected: cid={d}\n", .{e.cid});
                    for (&players, 0..) |*p, i| {
                        if (p.cid == null) {
                            p.* = .{ .cid = e.cid, .last_heartbeat_ms = now_ms };
                            var buf: [proto.ASSIGN_SLOT_SIZE]u8 = undefined;
                            proto.encodeAssignSlot(&buf, @intCast(i));
                            srv.sendOnChannel(e.cid, proto.CH_ACTION, &buf) catch {};
                            std.debug.print("  Assigned slot {d}\n", .{i});
                            break;
                        }
                    }
                },
                .ClientDisconnected => |e| {
                    std.debug.print("Client disconnected: cid={d}\n", .{e.cid});
                    for (&players, 0..) |*p, i| {
                        if (p.cid == e.cid) {
                            if (!game_over) notifyGameOver(&srv, @intCast(i ^ 1));
                            p.* = .{};
                            game_over = true;
                            break;
                        }
                    }
                },
            }
        }

        // peekMessage returns a zero-copy view without removing it from the queue.
        // consumeMessage advances the ring buffer. msg.cid identifies the sender.
        // msg.channel_id is the index into net_opts.channels.
        while (srv.peekMessage()) |msg| {
            const data = srv.messageData(msg);
            switch (msg.channel_id) {
                proto.CH_ACTION => {
                    if (data.len == 0) {
                        srv.consumeMessage();
                        continue;
                    }
                    switch (data[0]) {
                        proto.TAG_READY => {
                            if (game_over) {
                                for (&players) |*p| {
                                    if (p.cid == msg.cid) p.ready = true;
                                }
                                if (players[0].cid != null and players[0].ready and
                                    players[1].cid != null and players[1].ready)
                                {
                                    startNewGame(&srv);
                                }
                            }
                        },
                        else => {},
                    }
                },
                proto.CH_INPUT => {
                    if (data.len == 0) {
                        srv.consumeMessage();
                        continue;
                    }
                    switch (data[0]) {
                        proto.TAG_PLAYER_INPUT => {
                            if (proto.decodePlayerInput(data)) |dir| {
                                for (&players) |*p| {
                                    if (p.cid == msg.cid) {
                                        p.dir = dir;
                                        p.last_heartbeat_ms = now_ms;
                                    }
                                }
                            }
                        },
                        else => {},
                    }
                },
                proto.CH_CHAT => {
                    for (players) |p| {
                        if (p.cid) |cid| srv.sendOnChannel(cid, proto.CH_CHAT, data) catch {};
                    }
                },
                else => {},
            }
            srv.consumeMessage();
        }

        const both_connected = players[0].cid != null and players[1].cid != null;
        if (both_connected and !prev_both_connected) resetGame();
        prev_both_connected = both_connected;

        if (both_connected and !game_over) {
            for (&players) |*p| {
                p.y += @as(f32, @floatFromInt(p.dir)) * proto.PADDLE_SPEED * dt;
                p.y = std.math.clamp(p.y, 0, proto.SCREEN_H - proto.PADDLE_H);
            }

            ball_x += vel_x * dt;
            ball_y += vel_y * dt;

            if (ball_y - proto.BALL_R < 0) {
                ball_y = proto.BALL_R;
                vel_y = @abs(vel_y);
            }
            if (ball_y + proto.BALL_R > proto.SCREEN_H) {
                ball_y = proto.SCREEN_H - proto.BALL_R;
                vel_y = -@abs(vel_y);
            }

            const lx = proto.PADDLE_MARGIN;
            const rx = proto.SCREEN_W - proto.PADDLE_MARGIN - proto.PADDLE_W;
            if (vel_x < 0 and ball_x - proto.BALL_R <= lx + proto.PADDLE_W and
                ball_y >= players[0].y and ball_y <= players[0].y + proto.PADDLE_H)
            {
                vel_x = @abs(vel_x) * 1.05;
                ball_x = lx + proto.PADDLE_W + proto.BALL_R;
                vel_y = ((ball_y - players[0].y) / proto.PADDLE_H - 0.5) * 2.0 * proto.BALL_SPEED;
            }
            if (vel_x > 0 and ball_x + proto.BALL_R >= rx and
                ball_y >= players[1].y and ball_y <= players[1].y + proto.PADDLE_H)
            {
                vel_x = -@abs(vel_x) * 1.05;
                ball_x = rx - proto.BALL_R;
                vel_y = ((ball_y - players[1].y) / proto.PADDLE_H - 0.5) * 2.0 * proto.BALL_SPEED;
            }

            if (ball_x < 0) {
                score[1] += 1;
                resetBall(-1);
            }
            if (ball_x > proto.SCREEN_W) {
                score[0] += 1;
                resetBall(1);
            }

            if (score[0] >= proto.MAX_SCORE or score[1] >= proto.MAX_SCORE) {
                game_over = true;
                const winner: u8 = if (score[0] >= proto.MAX_SCORE) 0 else 1;
                var buf: [proto.GAME_OVER_SIZE]u8 = undefined;
                proto.encodeGameOver(&buf, winner);
                for (players) |p| {
                    if (p.cid) |cid| srv.sendOnChannel(cid, proto.CH_ACTION, &buf) catch {};
                }
            }
        }

        if (both_connected) {
            var ball_buf: [proto.BALL_STATE_SIZE]u8 = undefined;
            proto.encodeBallState(&ball_buf, .{
                .ball_x = ball_x,
                .ball_y = ball_y,
                .vel_x = vel_x,
                .vel_y = vel_y,
                .score_left = score[0],
                .score_right = score[1],
            });
            for (players) |p| {
                if (p.cid) |cid| srv.sendOnChannel(cid, proto.CH_BALL, &ball_buf) catch {};
            }
            for (players, 0..) |p, i| {
                var pad_buf: [proto.PADDLE_STATE_SIZE]u8 = undefined;
                proto.encodePaddleState(&pad_buf, @intCast(i), p.y);
                for (players) |p2| {
                    if (p2.cid) |cid| srv.sendOnChannel(cid, proto.CH_PADDLE, &pad_buf) catch {};
                }
            }
        }

        try sleepNs(16 * std.time.ns_per_ms);
    }
}

fn notifyGameOver(srv: *Srv, winner_slot: u8) void {
    if (winner_slot >= 2) return;
    if (players[winner_slot].cid) |cid| {
        var buf: [proto.GAME_OVER_SIZE]u8 = undefined;
        proto.encodeGameOver(&buf, winner_slot);
        srv.sendOnChannel(cid, proto.CH_ACTION, &buf) catch {};
    }
}

fn startNewGame(srv: *Srv) void {
    resetGame();
    const ng = [1]u8{proto.TAG_NEW_GAME};
    for (players) |p| {
        if (p.cid) |cid| srv.sendOnChannel(cid, proto.CH_ACTION, &ng) catch {};
    }
}

fn resetGame() void {
    score = .{ 0, 0 };
    game_over = false;
    for (&players) |*p| {
        p.y = proto.SCREEN_H / 2 - proto.PADDLE_H / 2;
        p.dir = 0;
        p.ready = false;
    }
    resetBall(1);
}

fn resetBall(dir: f32) void {
    ball_x = proto.SCREEN_W / 2;
    ball_y = proto.SCREEN_H / 2;
    vel_x = proto.BALL_SPEED * dir;
    vel_y = proto.BALL_SPEED * 0.4;
}
