const std = @import("std");
const zanoza = @import("zanoza");

// zanoza.Options is a comptime struct; both sides must use the exact same value.
// It controls wire format, queue sizes, and which channels exist.
pub const net_opts: zanoza.Options = .{
    .max_clients = 4,
    .max_payload_size = 256,
    .user_data_size = 0,
    // Session/control data should be reliable and ordered.
    // High-frequency position updates should only keep the newest value.
    .channels = &.{
        .{ .kind = .ReliableOrdered }, // ch 0 — session events (assign, remove)
        .{ .kind = .UnreliableLatest }, // ch 1 — ball positions
        .{ .kind = .Unreliable }, // ch 2 — chat: fire-and-forget, lowest overhead
    },
    .reliable_resend_ns = 80 * std.time.ns_per_ms,
    .messages_queue_size = 128,
    .events_queue_size = 16,
    .outgoing_queue_size = 128,
};

// Must fit in u32 — the wire format stores only the lower 32 bits.
pub const PROTOCOL_ID: u64 = 0x42414C4C; // "BALL"
pub const SERVER_PORT: u16 = 9914;

pub const CH_SESSION: u8 = 0;
pub const CH_BALL: u8 = 1;
pub const CH_CHAT: u8 = 2;

pub const W: f32 = 960;
pub const H: f32 = 720;
pub const BALL_R: f32 = 16;
pub const SPEED: f32 = 220;

// First byte of every message identifies its type (application-level framing).
pub const TAG_ASSIGN: u8 = 0x01; // server → client: your slot id + color index
pub const TAG_BALL:   u8 = 0x10; // client → server, server → others: ball position
pub const TAG_REMOVE: u8 = 0x02; // server → clients: a peer disconnected
pub const TAG_CHAT:   u8 = 0x20; // both directions: chat text

pub const MAX_CHAT: usize = 128;

// Floats encoded as little-endian u32 bit patterns — avoids alignment-cast UB
// since zanoza delivers payloads as []u8 with no alignment guarantee.
fn writeF32(buf: []u8, v: f32) void { std.mem.writeInt(u32, buf[0..4], @bitCast(v), .little); }
fn readF32(buf: []const u8) f32 { return @bitCast(std.mem.readInt(u32, buf[0..4], .little)); }

// TAG_ASSIGN: [TAG(1), id(1)] = 2 bytes
// Color is derived from id (id % 4) so no extra field needed.
pub const ASSIGN_SIZE: usize = 2;
pub fn encodeAssign(buf: *[ASSIGN_SIZE]u8, id: u8) void { buf[0] = TAG_ASSIGN; buf[1] = id; }
pub fn decodeAssign(data: []const u8) ?u8 {
    if (data.len < ASSIGN_SIZE or data[0] != TAG_ASSIGN) return null;
    return data[1];
}

// TAG_BALL: [TAG(1), id(1), x(4), y(4)] = 10 bytes
pub const BALL_SIZE: usize = 10;
pub fn encodeBall(buf: *[BALL_SIZE]u8, id: u8, x: f32, y: f32) void {
    buf[0] = TAG_BALL; buf[1] = id;
    writeF32(buf[2..6], x); writeF32(buf[6..10], y);
}
pub fn decodeBall(data: []const u8) ?struct { id: u8, x: f32, y: f32 } {
    if (data.len < BALL_SIZE or data[0] != TAG_BALL) return null;
    return .{ .id = data[1], .x = readF32(data[2..6]), .y = readF32(data[6..10]) };
}

// TAG_REMOVE: [TAG(1), id(1)] = 2 bytes
pub const REMOVE_SIZE: usize = 2;
pub fn encodeRemove(buf: *[REMOVE_SIZE]u8, id: u8) void { buf[0] = TAG_REMOVE; buf[1] = id; }
pub fn decodeRemove(data: []const u8) ?u8 {
    if (data.len < REMOVE_SIZE or data[0] != TAG_REMOVE) return null;
    return data[1];
}

// TAG_CHAT: [TAG(1), len(1), UTF-8 text] — variable length
pub fn encodeChat(buf: []u8, text: []const u8) []const u8 {
    const len = @min(text.len, @min(MAX_CHAT, buf.len - 2));
    buf[0] = TAG_CHAT; buf[1] = @intCast(len);
    @memcpy(buf[2..2 + len], text[0..len]);
    return buf[0..2 + len];
}
pub fn decodeChat(data: []const u8) ?[]const u8 {
    if (data.len < 2 or data[0] != TAG_CHAT) return null;
    const len: usize = data[1];
    if (data.len < 2 + len) return null;
    return data[2..2 + len];
}
