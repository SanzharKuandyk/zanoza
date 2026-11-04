const std = @import("std");
const zanoza = @import("zanoza");

// zanoza.Options is a comptime struct shared by TransportServer and TransportClient.
// Both sides must use the exact same value — it controls wire format, queue sizes,
// and which channels exist. Mismatching options means packets can't be parsed.
pub const net_opts: zanoza.Options = .{
    .max_clients = 4,
    .max_payload_size = 512,
    .user_data_size = 64,
    // Each entry in `channels` is one logical stream with its own delivery guarantee.
    // The index becomes the channel_id passed to sendOnChannel / visible in Message.channel_id.
    .channels = &.{
        .{ .kind = .UnreliableLatest }, // ch 0 — ball state (server → clients)
        .{ .kind = .UnreliableLatest }, // ch 1 — paddle state (server → clients)
        .{ .kind = .ReliableOrdered, .reliable_buffer = 32 }, // ch 2 — actions/events
        .{ .kind = .UnreliableLatest }, // ch 3 — player input / heartbeat (client → server)
        .{ .kind = .Unreliable }, // ch 4 — chat: fire-and-forget
    },
    // Resend a reliable message if its ACK hasn't arrived within this window.
    .reliable_resend_ns = 80 * std.time.ns_per_ms,
    .messages_queue_size = 128,
    .events_queue_size = 64,
    .outgoing_queue_size = 128,
};

// Must match between server and client. The wire format stores only the lower 32 bits,
// so the value must fit in u32 or the server will reject every ConnectionRequest.
pub const PROTOCOL_ID: u64 = 0x504F4E47; // "PONG"
pub const SERVER_PORT: u16 = 9913;

pub const CH_BALL: u8 = 0;
pub const CH_PADDLE: u8 = 1;
pub const CH_ACTION: u8 = 2;
pub const CH_INPUT: u8 = 3;
pub const CH_CHAT: u8 = 4;

pub const SCREEN_W: f32 = 800;
pub const SCREEN_H: f32 = 600;
pub const PADDLE_W: f32 = 12;
pub const PADDLE_H: f32 = 80;
pub const BALL_R: f32 = 8;
pub const PADDLE_SPEED: f32 = 400;
pub const BALL_SPEED: f32 = 350;
pub const PADDLE_MARGIN: f32 = 30;
pub const MAX_SCORE: u8 = 11;

// First byte of every message payload identifies its type.
// zanoza has no built-in message typing — this is application-level framing on top of
// the raw byte slice exposed by peekMessage/pollMessage.
pub const TAG_BALL_STATE: u8 = 0x01;  // server → clients, ch 0
pub const TAG_PADDLE_STATE: u8 = 0x10; // server → clients, ch 1
pub const TAG_PLAYER_INPUT: u8 = 0x20; // client → server, ch 3; also acts as heartbeat
pub const TAG_ASSIGN_SLOT: u8 = 0x30;  // server → client, ch 2; sent on ClientConnected
pub const TAG_GAME_OVER: u8 = 0x40;    // server → clients, ch 2
pub const TAG_READY: u8 = 0x41;        // client → server, ch 2; "I want to play again"
pub const TAG_NEW_GAME: u8 = 0x42;     // server → clients, ch 2; clears game-over on client
pub const TAG_CHAT: u8 = 0x50;         // both directions, ch 4

// Floats are encoded as little-endian u32 bit patterns to avoid alignment-cast UB.
// zanoza delivers payloads as []u8 slices with no alignment guarantee, so @ptrCast
// to a float pointer would be undefined behaviour on some targets.
fn writeF32(buf: []u8, v: f32) void {
    std.mem.writeInt(u32, buf[0..4], @bitCast(v), .little);
}
fn readF32(buf: []const u8) f32 {
    return @bitCast(std.mem.readInt(u32, buf[0..4], .little));
}

// tag(1) + ball_x(4) + ball_y(4) + vel_x(4) + vel_y(4) + score_l(1) + score_r(1) = 19
pub const BALL_STATE_SIZE: usize = 19;

pub const BallState = struct {
    ball_x: f32, ball_y: f32,
    vel_x: f32,  vel_y: f32,
    score_left: u8, score_right: u8,
};

pub fn encodeBallState(buf: *[BALL_STATE_SIZE]u8, s: BallState) void {
    buf[0] = TAG_BALL_STATE;
    writeF32(buf[1..5],   s.ball_x);
    writeF32(buf[5..9],   s.ball_y);
    writeF32(buf[9..13],  s.vel_x);
    writeF32(buf[13..17], s.vel_y);
    buf[17] = s.score_left;
    buf[18] = s.score_right;
}

pub fn decodeBallState(data: []const u8) ?BallState {
    if (data.len < BALL_STATE_SIZE or data[0] != TAG_BALL_STATE) return null;
    return .{
        .ball_x = readF32(data[1..5]),   .ball_y = readF32(data[5..9]),
        .vel_x  = readF32(data[9..13]),  .vel_y  = readF32(data[13..17]),
        .score_left = data[17], .score_right = data[18],
    };
}

// tag(1) + slot(1) + y(4) = 6
pub const PADDLE_STATE_SIZE: usize = 6;

pub fn encodePaddleState(buf: *[PADDLE_STATE_SIZE]u8, slot: u8, y: f32) void {
    buf[0] = TAG_PADDLE_STATE;
    buf[1] = slot;
    writeF32(buf[2..6], y);
}
pub fn decodePaddleSlot(data: []const u8) ?u8  { if (data.len < PADDLE_STATE_SIZE or data[0] != TAG_PADDLE_STATE) return null; return data[1]; }
pub fn decodePaddleY(data: []const u8)   ?f32  { if (data.len < PADDLE_STATE_SIZE or data[0] != TAG_PADDLE_STATE) return null; return readF32(data[2..6]); }

// tag(1) + direction(1) = 2  (-1 / 0 / +1 as i8)
pub const PLAYER_INPUT_SIZE: usize = 2;

pub fn encodePlayerInput(buf: *[PLAYER_INPUT_SIZE]u8, direction: i8) void { buf[0] = TAG_PLAYER_INPUT; buf[1] = @bitCast(direction); }
pub fn decodePlayerInput(data: []const u8) ?i8 { if (data.len < PLAYER_INPUT_SIZE or data[0] != TAG_PLAYER_INPUT) return null; return @bitCast(data[1]); }

// tag(1) + slot(1) = 2
pub const ASSIGN_SLOT_SIZE: usize = 2;

pub fn encodeAssignSlot(buf: *[ASSIGN_SLOT_SIZE]u8, slot: u8) void { buf[0] = TAG_ASSIGN_SLOT; buf[1] = slot; }
pub fn decodeAssignSlot(data: []const u8) ?u8 { if (data.len < ASSIGN_SLOT_SIZE or data[0] != TAG_ASSIGN_SLOT) return null; return data[1]; }

// tag(1) + winner_slot(1) = 2
pub const GAME_OVER_SIZE: usize = 2;

pub fn encodeGameOver(buf: *[GAME_OVER_SIZE]u8, winner_slot: u8) void { buf[0] = TAG_GAME_OVER; buf[1] = winner_slot; }
pub fn decodeGameOver(data: []const u8) ?u8 { if (data.len < GAME_OVER_SIZE or data[0] != TAG_GAME_OVER) return null; return data[1]; }

// tag(1) + len(1) + UTF-8 text
pub const MAX_CHAT_LEN: usize = 200;

pub fn encodeChatMsg(buf: []u8, text: []const u8) []const u8 {
    const len = @min(text.len, @min(MAX_CHAT_LEN, buf.len - 2));
    buf[0] = TAG_CHAT;
    buf[1] = @intCast(len);
    @memcpy(buf[2 .. 2 + len], text[0..len]);
    return buf[0 .. 2 + len];
}

pub fn decodeChatMsg(data: []const u8) ?[]const u8 {
    if (data.len < 2 or data[0] != TAG_CHAT) return null;
    const len: usize = data[1];
    if (data.len < 2 + len) return null;
    return data[2 .. 2 + len];
}
