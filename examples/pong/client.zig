const std = @import("std");
const rl = @import("raylib");
const zenet = @import("zenet");
const proto = @import("protocol");

// TransportClient(opts, void) — `void` selects the built-in UDP socket.
// The opts must be identical to what the server was compiled with.
const Cli = zenet.TransportClient(proto.net_opts, void);

const W = @as(i32, @intFromFloat(proto.SCREEN_W));
const H = @as(i32, @intFromFloat(proto.SCREEN_H));

// All mutable game state lives here so it can be zeroed in one shot on reconnect.
const GameState = struct {
    slot: ?u8 = null,
    ball_x: f32 = proto.SCREEN_W / 2,
    ball_y: f32 = proto.SCREEN_H / 2,
    score: [2]u8 = .{ 0, 0 },
    paddles: [2]f32 = .{
        proto.SCREEN_H / 2 - proto.PADDLE_H / 2,
        proto.SCREEN_H / 2 - proto.PADDLE_H / 2,
    },
    winner: ?u8 = null,
    ready_sent: bool = false, // TAG_READY was sent; waiting for the other player
};

var game: GameState = .{};
var connected = false;

const MAX_LINES = 12;
const ChatLine = struct { buf: [proto.MAX_CHAT_LEN]u8 = undefined, len: usize = 0 };
var chat_lines: [MAX_LINES]ChatLine = @splat(.{});
var chat_count: usize = 0;
var chat_input: [proto.MAX_CHAT_LEN]u8 = undefined;
var chat_input_len: usize = 0;
var chat_active = false;

pub fn main(init: std.process.Init) !void {
    var server_ip: [4]u8 = .{ 127, 0, 0, 1 };
    {
        var args = try std.process.Args.iterateAllocator(init.minimal.args, init.gpa);
        defer args.deinit();
        _ = args.next();
        if (args.next()) |s| server_ip = parseIp4(s) orelse .{ 127, 0, 0, 1 };
    }

    const client_config: zenet.ClientConfig = .{
        .protocol_id = proto.PROTOCOL_ID,
        .server_addr = .{ .ip4 = .{ .bytes = server_ip, .port = proto.SERVER_PORT } },
        // How long to wait for the handshake to complete before giving up.
        .connect_timeout_ns = 10 * std.time.ns_per_s,
        // How long a connected client can go without receiving a packet before
        // the client-side state machine emits a Disconnected event.
        .timeout_ns = 30 * std.time.ns_per_s,
    };

    // init opens and binds a UDP socket. Port 0 lets the OS pick a free port.
    var cli = try Cli.init(client_config, .{ .ip4 = .unspecified(0) });
    defer cli.deinit();
    // Best-effort clean disconnect on exit: sends a Disconnect packet so the server
    // frees the slot immediately instead of waiting for the heartbeat timeout.
    defer {
        cli.disconnect();
        cli.tick();
    }

    // connect() queues a ConnectionRequest; the 3-way handshake completes on the
    // first few ticks and fires a Connected event when done.
    try cli.connect();

    rl.initWindow(W, H, "Zenet Pong");
    defer rl.closeWindow();
    rl.setTargetFPS(60);

    while (!rl.windowShouldClose()) {
        // tick() drives the client: recv datagrams → state machine → retransmit
        // reliable messages → flush outgoing packets. Must be called every frame.
        cli.tick();

        // pollEvent returns Connected or Disconnected lifecycle events.
        while (cli.pollEvent()) |ev| {
            switch (ev) {
                .Connected => {
                    connected = true;
                    game = .{}; // clear all game state for the new session
                    appendChat("Connected!");
                },
                .Disconnected => {
                    connected = false;
                    appendChat("Disconnected.");
                },
            }
        }

        // peekMessage / consumeMessage: zero-copy ring-buffer access.
        // The pointer is valid until consumeMessage is called.
        while (cli.peekMessage()) |msg| {
            handleMessage(&cli, msg);
            cli.consumeMessage();
        }

        if (connected and game.slot != null) {
            if (!chat_active) {
                var dir: i8 = 0;
                if (rl.isKeyDown(.w) or rl.isKeyDown(.up)) dir = -1;
                if (rl.isKeyDown(.s) or rl.isKeyDown(.down)) dir = 1;

                // Client-side prediction: move our paddle locally for instant feedback.
                // The server's authoritative position overrides this when it arrives.
                if (game.slot) |s| {
                    game.paddles[s] += @as(f32, @floatFromInt(dir)) * proto.PADDLE_SPEED * rl.getFrameTime();
                    game.paddles[s] = std.math.clamp(game.paddles[s], 0, proto.SCREEN_H - proto.PADDLE_H);
                }

                var buf: [proto.PLAYER_INPUT_SIZE]u8 = undefined;
                proto.encodePlayerInput(&buf, dir);
                // CH_INPUT is UnreliableLatest: only the newest direction matters.
                cli.sendOnChannel(proto.CH_INPUT, &buf) catch {};
            }

            if (game.winner != null and !game.ready_sent and rl.isKeyPressed(.space)) {
                // TAG_READY: server waits for both players then broadcasts TAG_NEW_GAME.
                const msg = [1]u8{proto.TAG_READY};
                cli.sendOnChannel(proto.CH_ACTION, &msg) catch {};
                game.ready_sent = true;
            }

            if (!chat_active and rl.isKeyPressed(.t)) {
                chat_active = true;
                chat_input_len = 0;
                _ = rl.getCharPressed(); // discard the 't' that triggered this
            }
            if (chat_active) handleChatInput(&cli);
        }

        rl.beginDrawing();
        defer rl.endDrawing();
        rl.clearBackground(.{ .r = 20, .g = 20, .b = 30, .a = 255 });

        if (!connected) {
            drawText("Connecting...", @divFloor(W, 2), @divFloor(H, 2), 30, .white, .center);
        } else if (game.slot == null) {
            drawText("Waiting for second player...", @divFloor(W, 2), @divFloor(H, 2), 20, .white, .center);
        } else if (game.winner) |winner| {
            const msg = if (winner == game.slot.?) "YOU WIN!" else "YOU LOSE";
            drawText(msg, @divFloor(W, 2), @divFloor(H, 2) - 40, 50, .yellow, .center);
            const sub = if (game.ready_sent) "Waiting for opponent..." else "SPACE to play again";
            drawText(sub, @divFloor(W, 2), @divFloor(H, 2) + 20, 20, .gray, .center);
        } else {
            drawGame();
        }

        drawChat();
    }
}

fn handleMessage(cli: *const Cli, msg: *const Cli.MessageView) void {
    const data = cli.messageData(msg);
    if (data.len == 0) return;
    // msg.channel_id maps to net_opts.channels[i]; messageData(msg) is the raw payload.
    switch (msg.channel_id) {
        proto.CH_BALL => {
            if (proto.decodeBallState(data)) |bs| {
                game.ball_x = bs.ball_x;
                game.ball_y = bs.ball_y;
                game.score = .{ bs.score_left, bs.score_right };
            }
        },
        proto.CH_PADDLE => switch (data[0]) {
            proto.TAG_PADDLE_STATE => {
                const slot = proto.decodePaddleSlot(data) orelse return;
                const y = proto.decodePaddleY(data) orelse return;
                if (slot < 2) game.paddles[slot] = y;
            },
            else => {},
        },
        proto.CH_ACTION => switch (data[0]) {
            proto.TAG_ASSIGN_SLOT => {
                const slot = proto.decodeAssignSlot(data) orelse return;
                game.slot = slot;
                var buf: [32]u8 = undefined;
                appendChat(std.fmt.bufPrint(&buf, "You are Player {d} ({s})", .{
                    @as(u16, slot) + 1, if (slot == 0) "LEFT" else "RIGHT",
                }) catch "Assigned");
            },
            proto.TAG_GAME_OVER => {
                game.winner = proto.decodeGameOver(data);
                game.ready_sent = false;
            },
            proto.TAG_NEW_GAME => {
                // Server confirmed both players are ready; clear game-over state.
                game.winner = null;
                game.ready_sent = false;
                game.score = .{ 0, 0 };
            },
            else => {},
        },
        proto.CH_CHAT => {
            if (proto.decodeChatMsg(data)) |text| appendChat(text);
        },
        else => {},
    }
}

fn drawGame() void {
    var yy: i32 = 0;
    while (yy < H) : (yy += 20)
        rl.drawRectangle(@divFloor(W, 2) - 1, yy, 2, 10, .{ .r = 60, .g = 60, .b = 80, .a = 255 });

    var sbuf: [16]u8 = undefined;
    drawText(std.fmt.bufPrint(&sbuf, "{d}", .{game.score[0]}) catch "?", @divFloor(W, 4), 20, 40, .white, .center);
    drawText(std.fmt.bufPrint(&sbuf, "{d}", .{game.score[1]}) catch "?", @divFloor(W, 4) * 3, 20, 40, .white, .center);

    const lx: i32 = @intFromFloat(proto.PADDLE_MARGIN);
    const rx: i32 = @intFromFloat(proto.SCREEN_W - proto.PADDLE_MARGIN - proto.PADDLE_W);
    const pw: i32 = @intFromFloat(proto.PADDLE_W);
    const ph: i32 = @intFromFloat(proto.PADDLE_H);
    rl.drawRectangle(lx, @intFromFloat(game.paddles[0]), pw, ph, .white);
    rl.drawRectangle(rx, @intFromFloat(game.paddles[1]), pw, ph, .white);

    if (game.slot) |slot| {
        const px = if (slot == 0) lx else rx;
        rl.drawRectangleLines(px - 2, @as(i32, @intFromFloat(game.paddles[slot])) - 2, pw + 4, ph + 4, .green);
    }

    rl.drawCircle(@intFromFloat(game.ball_x), @intFromFloat(game.ball_y), proto.BALL_R, .white);

    const hint: []const u8 = if (chat_active) "Enter=send  Esc=cancel" else "W/S=move  T=chat";
    drawText(hint, 10, H - 14, 10, .{ .r = 100, .g = 100, .b = 120, .a = 255 }, .left);
}

fn drawChat() void {
    const base_y = H - 34;
    if (chat_active) {
        var ibuf: [proto.MAX_CHAT_LEN + 4]u8 = undefined;
        drawText(std.fmt.bufPrint(&ibuf, "> {s}_", .{chat_input[0..chat_input_len]}) catch "> _", 10, base_y, 12, .green, .left);
    }
    const visible = @min(chat_count, 10);
    for (0..visible) |vi| {
        const idx = (chat_count - visible + vi) % chat_lines.len;
        const line = &chat_lines[idx];
        if (line.len == 0) continue;
        const ly = base_y - @as(i32, @intCast(visible - vi)) * 13 - (if (chat_active) @as(i32, 15) else 0);
        drawText(line.buf[0..line.len], 10, ly, 11, .{ .r = 170, .g = 170, .b = 190, .a = 200 }, .left);
    }
}

// raylib requires null-terminated strings; this helper accepts a plain slice,
// copies it into a stack buffer with a null sentinel, then calls rl.drawText.
const Align = enum { left, center };
fn drawText(text: []const u8, x: i32, y: i32, size: i32, color: rl.Color, alignment: Align) void {
    var buf: [256]u8 = undefined;
    const len = @min(text.len, buf.len - 1);
    @memcpy(buf[0..len], text[0..len]);
    buf[len] = 0;
    const s: [:0]const u8 = buf[0..len :0];
    const px = if (alignment == .center) x - @divFloor(rl.measureText(s, size), 2) else x;
    rl.drawText(s, px, y, size, color);
}

fn handleChatInput(cli: *Cli) void {
    if (rl.isKeyPressed(.escape)) {
        chat_active = false;
        return;
    }
    if (rl.isKeyPressed(.enter) or rl.isKeyPressed(.kp_enter)) {
        if (chat_input_len > 0) {
            var buf: [proto.MAX_CHAT_LEN + 2]u8 = undefined;
            // CH_CHAT is Unreliable: no retransmit, no ACK, lowest overhead.
            cli.sendOnChannel(proto.CH_CHAT, proto.encodeChatMsg(&buf, chat_input[0..chat_input_len])) catch {};
        }
        chat_active = false;
        chat_input_len = 0;
        return;
    }
    if (rl.isKeyPressed(.backspace)) {
        if (chat_input_len > 0) chat_input_len -= 1;
        return;
    }
    const ch = rl.getCharPressed();
    if (ch > 0 and ch < 128 and chat_input_len < proto.MAX_CHAT_LEN) {
        chat_input[chat_input_len] = @intCast(@as(u32, @bitCast(ch)));
        chat_input_len += 1;
    }
}

fn appendChat(text: []const u8) void {
    const idx = chat_count % chat_lines.len;
    const len = @min(text.len, proto.MAX_CHAT_LEN);
    @memcpy(chat_lines[idx].buf[0..len], text[0..len]);
    chat_lines[idx].len = len;
    chat_count += 1;
}

fn parseIp4(s: []const u8) ?[4]u8 {
    var r: [4]u8 = undefined;
    var i: usize = 0;
    var part: u16 = 0;
    var digits: usize = 0;
    for (s) |c| {
        if (c == '.') {
            if (digits == 0 or i >= 3 or part > 255) return null;
            r[i] = @intCast(part);
            i += 1;
            part = 0;
            digits = 0;
        } else if (c >= '0' and c <= '9') {
            part = part * 10 + (c - '0');
            digits += 1;
        } else return null;
    }
    if (digits == 0 or i != 3 or part > 255) return null;
    r[3] = @intCast(part);
    return r;
}
