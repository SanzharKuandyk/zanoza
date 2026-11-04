const std = @import("std");
const rl = @import("raylib");
const zenet = @import("zenet");
const proto = @import("protocol");

// TransportClient(opts, void) — `void` selects the built-in UDP socket.
// opts must be identical to what the server was compiled with.
const Cli = zenet.TransportClient(proto.net_opts, void);

const W = @as(i32, @intFromFloat(proto.W));
const H = @as(i32, @intFromFloat(proto.H));

// Each slot has a distinct color; derived from id so no extra wire field is needed.
const COLORS = [4]rl.Color{ .white, .red, .{ .r = 0, .g = 210, .b = 60, .a = 255 }, .sky_blue };

const Ball = struct {
    active: bool = false,
    x: f32 = proto.W / 2,
    y: f32 = proto.H / 2,
};

var my_id: ?u8 = null;
var balls: [4]Ball = @splat(.{});
var connected = false;

const MAX_LINES = 8;
const ChatLine = struct { buf: [proto.MAX_CHAT]u8 = undefined, len: usize = 0 };
var chat_lines: [MAX_LINES]ChatLine = @splat(.{});
var chat_count: usize = 0;
var chat_input: [proto.MAX_CHAT]u8 = undefined;
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

    const config: zenet.ClientConfig = .{
        .protocol_id = proto.PROTOCOL_ID,
        .server_addr = .{ .ip4 = .{ .bytes = server_ip, .port = proto.SERVER_PORT } },
        // How long to wait for the handshake before giving up.
        .connect_timeout_ns = 10 * std.time.ns_per_s,
        // How long without a packet before the state machine fires Disconnected.
        .timeout_ns = 30 * std.time.ns_per_s,
    };

    // init opens and binds a UDP socket. Port 0 lets the OS pick a free port.
    var cli = try Cli.init(config, .{ .ip4 = .unspecified(0) });
    defer cli.deinit();
    // Best-effort clean disconnect: sends a Disconnect packet so the server frees
    // the slot immediately rather than waiting for the heartbeat timeout.
    defer {
        cli.disconnect();
        cli.tick();
    }

    // connect() queues a ConnectionRequest; the handshake completes over the first
    // few ticks and fires a Connected event when done.
    try cli.connect();

    rl.initWindow(W, H, "Zenet Ball");
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
                    my_id = null;
                    balls = @splat(.{}); // clear peer state for the new session
                    appendChat("Connected!");
                },
                .Disconnected => {
                    connected = false;
                    my_id = null;
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

        if (connected) {
            if (!chat_active) handleMovement(&cli);
            if (!chat_active and rl.isKeyPressed(.t)) {
                chat_active = true;
                chat_input_len = 0;
                _ = rl.getCharPressed(); // discard the 't' that triggered this
            }
            if (chat_active) handleChatInput(&cli);
        }

        rl.beginDrawing();
        defer rl.endDrawing();
        rl.clearBackground(.{ .r = 15, .g = 15, .b = 25, .a = 255 });

        if (!connected) {
            drawText("Connecting...", W / 2, H / 2, 24, .white, .center);
        } else if (my_id == null) {
            drawText("Waiting for slot...", W / 2, H / 2, 20, .white, .center);
        } else {
            drawBalls();
        }

        drawChat();
    }
}

fn handleMessage(cli: *const Cli, msg: *const Cli.MessageView) void {
    const data = cli.messageData(msg);
    if (data.len == 0) return;
    switch (msg.channel_id) {
        proto.CH_SESSION => switch (data[0]) {
            proto.TAG_ASSIGN => {
                if (proto.decodeAssign(data)) |id| {
                    my_id = id;
                    if (id < balls.len) balls[id] = .{ .active = true };
                    var buf: [32]u8 = undefined;
                    appendChat(std.fmt.bufPrint(&buf, "You are ball {d}", .{id}) catch "Assigned");
                }
            },
            proto.TAG_BALL => {
                if (proto.decodeBall(data)) |b| {
                    if (b.id < balls.len) {
                        balls[b.id].active = true;
                        balls[b.id].x = b.x;
                        balls[b.id].y = b.y;
                    }
                }
            },
            proto.TAG_REMOVE => {
                if (proto.decodeRemove(data)) |id| {
                    if (id < balls.len) balls[id] = .{};
                    var buf: [32]u8 = undefined;
                    appendChat(std.fmt.bufPrint(&buf, "Ball {d} left", .{id}) catch "Left");
                }
            },
            else => {},
        },
        proto.CH_BALL => switch (data[0]) {
            proto.TAG_BALL => {
                if (proto.decodeBall(data)) |b| {
                    if (b.id < balls.len) {
                        balls[b.id].active = true;
                        balls[b.id].x = b.x;
                        balls[b.id].y = b.y;
                    }
                }
            },
            else => {},
        },
        proto.CH_CHAT => {
            if (proto.decodeChat(data)) |text| appendChat(text);
        },
        else => {},
    }
}

fn handleMovement(cli: *Cli) void {
    const id = my_id orelse return;
    if (id >= balls.len) return;
    const b = &balls[id];
    const dt = rl.getFrameTime();

    if (rl.isKeyDown(.w) or rl.isKeyDown(.up)) b.y -= proto.SPEED * dt;
    if (rl.isKeyDown(.s) or rl.isKeyDown(.down)) b.y += proto.SPEED * dt;
    if (rl.isKeyDown(.a) or rl.isKeyDown(.left)) b.x -= proto.SPEED * dt;
    if (rl.isKeyDown(.d) or rl.isKeyDown(.right)) b.x += proto.SPEED * dt;
    b.x = std.math.clamp(b.x, proto.BALL_R, proto.W - proto.BALL_R);
    b.y = std.math.clamp(b.y, proto.BALL_R, proto.H - proto.BALL_R);

    var buf: [proto.BALL_SIZE]u8 = undefined;
    proto.encodeBall(&buf, id, b.x, b.y);
    // CH_BALL is UnreliableLatest: only the newest position matters.
    cli.sendOnChannel(proto.CH_BALL, &buf) catch {};
}

fn drawBalls() void {
    for (balls, 0..) |b, i| {
        if (!b.active) continue;
        const color = COLORS[i % COLORS.len];
        rl.drawCircle(@intFromFloat(b.x), @intFromFloat(b.y), proto.BALL_R, color);
        // Draw an outline ring around the local player's ball.
        if (my_id != null and my_id.? == i)
            rl.drawCircleLines(@intFromFloat(b.x), @intFromFloat(b.y), proto.BALL_R + 3, color);
    }
    const hint: []const u8 = if (chat_active) "Enter=send  Esc=cancel" else "WASD=move  T=chat";
    drawText(hint, 8, H - 20, 14, .{ .r = 100, .g = 100, .b = 120, .a = 255 }, .left);
}

fn drawChat() void {
    const base_y = H - 30;
    if (chat_active) {
        var ibuf: [proto.MAX_CHAT + 4]u8 = undefined;
        drawText(std.fmt.bufPrint(&ibuf, "> {s}_", .{chat_input[0..chat_input_len]}) catch "> _", 8, base_y, 18, .green, .left);
    }
    const visible = @min(chat_count, 6);
    for (0..visible) |vi| {
        const idx = (chat_count - visible + vi) % chat_lines.len;
        const line = &chat_lines[idx];
        if (line.len == 0) continue;
        const ly = base_y - @as(i32, @intCast(visible - vi)) * 20 - (if (chat_active) @as(i32, 22) else 0);
        drawText(line.buf[0..line.len], 8, ly, 16, .{ .r = 170, .g = 170, .b = 190, .a = 200 }, .left);
    }
}

fn handleChatInput(cli: *Cli) void {
    if (rl.isKeyPressed(.escape)) {
        chat_active = false;
        return;
    }
    if (rl.isKeyPressed(.enter) or rl.isKeyPressed(.kp_enter)) {
        if (chat_input_len > 0) {
            var buf: [proto.MAX_CHAT + 2]u8 = undefined;
            // CH_CHAT is Unreliable: no retransmit, no ACK, lowest overhead.
            cli.sendOnChannel(proto.CH_CHAT, proto.encodeChat(&buf, chat_input[0..chat_input_len])) catch {};
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
    if (ch > 0 and ch < 128 and chat_input_len < proto.MAX_CHAT) {
        chat_input[chat_input_len] = @intCast(@as(u32, @bitCast(ch)));
        chat_input_len += 1;
    }
}

fn appendChat(text: []const u8) void {
    const idx = chat_count % chat_lines.len;
    const len = @min(text.len, proto.MAX_CHAT);
    @memcpy(chat_lines[idx].buf[0..len], text[0..len]);
    chat_lines[idx].len = len;
    chat_count += 1;
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
