// Handshake flow:
// 1. (client) ConnectionRequest
// 2. (server) Challenge
// 3. (client) ConnectionResponse
// 4. (server) ConnectionAccepted

const std = @import("std");
const handshake = @import("handshake.zig");
const root = @import("root.zig");
const validation = @import("validation/root.zig");
const Options = root.Options;

pub const ChallengeToken = handshake.ChallengeToken;

pub const PROTOCOL_VERSION: u8 = 1;

// Wire format:
//   byte 0   = protocol version
//   byte 1   = packet kind
//   bytes 2+ = kind-specific payload, all integer fields big-endian
//
// Packet layouts:
//   ConnectionRequestPlain  = [ver:u8][kind:u8][protocol_id:u32][client_nonce:u64]
//   ConnectionRequestSecure = [ver:u8][kind:u8][protocol_id:u32][client_nonce:u64][token]
//   Challenge               = [ver:u8][kind:u8][sequence:u64][expires_at:u64][token:16]
//   ConnectionResponse      = [ver:u8][kind:u8][sequence:u64][token:16]
//   ConnectionAccepted      = [ver:u8][kind:u8]
//   Payload                 = [ver:u8][kind:u8][flags:u8][channel:u8][seq:u16][len:u16][fragment?][body:len]
//   fragment                = [msg_id:u16][count:u8][index:u8][total_size:u16]
//   Disconnect              = [ver:u8][kind:u8]
//
// Sizes below count serialized bytes: u8 = 1, u16 = 2, u32 = 4, u64 = 8.

/// ver (1) + kind (1).
pub const PACKET_HEADER_SIZE: usize = 1 + 1;
/// Common header + protocol_id (4) + client_nonce (8). Secure requests append their token.
pub const CONNECTION_REQUEST_HEADER_SIZE: usize = PACKET_HEADER_SIZE + 4 + 8;
/// Common header + sequence (8) + expires_at (8) + challenge token (16).
pub const CHALLENGE_PACKET_SIZE: usize = PACKET_HEADER_SIZE + 8 + 8 + @sizeOf(ChallengeToken);
/// Common header + sequence (8) + challenge token (16).
pub const CONNECTION_RESPONSE_PACKET_SIZE: usize = PACKET_HEADER_SIZE + 8 + @sizeOf(ChallengeToken);

/// Common header + flags (1) + channel (1) + seq (2) + len (2).
pub const PAYLOAD_HEADER_SIZE: usize = PACKET_HEADER_SIZE + 1 + 1 + 2 + 2;
/// msg_id (2) + count (1) + index (1) + total_size (2).
pub const FRAGMENT_HEADER_SIZE: usize = 2 + 1 + 1 + 2;
/// Payload header followed by fragment metadata, before the user-data bytes.
pub const FRAGMENTED_PAYLOAD_HEADER_SIZE: usize = PAYLOAD_HEADER_SIZE + FRAGMENT_HEADER_SIZE;

const ACK_FLAG: u8 = 1;
const FRAGMENT_FLAG: u8 = 2;

/// Delivery metadata serialized ahead of the user-data bytes.
pub const PayloadHeader = struct {
    channel_id: u8 = 0,
    seq: u16 = 0,
    is_ack: bool = false,
    fragment: ?FragmentHeader = null,

    pub fn wireSize(self: PayloadHeader) usize {
        return if (self.fragment != null) FRAGMENTED_PAYLOAD_HEADER_SIZE else PAYLOAD_HEADER_SIZE;
    }
};

/// Describes one fragment of a complete user message.
pub const FragmentHeader = struct {
    msg_id: u16,
    frag_count: u8,
    frag_index: u8,
    total_size: u16,
};

pub const PacketKind = enum(u8) {
    ConnectionRequestPlain = 1,
    ConnectionRequestSecure = 2,
    Challenge = 3,
    ConnectionResponse = 4,
    ConnectionAccepted = 5,
    Payload = 6,
    Disconnect = 7,
};

fn TokenType(comptime opts: Options) type {
    return if (opts.ConnectToken == void)
        handshake.DefaultConnectToken(opts.user_data_size, opts.max_token_addresses)
    else
        opts.ConnectToken;
}

pub fn Packet(comptime opts: Options) type {
    comptime validation.options.validate(opts);

    return union(enum) {
        ConnectionRequest: ConnectionRequest(opts),
        Challenge: Challenge,
        ConnectionResponse: ConnectionResponse,
        ConnectionAccepted,
        Payload: Payload(opts),
        Disconnect,
    };
}

pub const SerializeError = error{ BufferTooSmall, PayloadTooLarge, InvalidPacket };
pub const DeserializeError = error{ InvalidPacket, UnsupportedVersion };

pub fn maxPacketSize(comptime opts: Options) usize {
    comptime validation.options.validate(opts);
    return @max(
        CONNECTION_REQUEST_HEADER_SIZE + TokenType(opts).wire_size,
        CHALLENGE_PACKET_SIZE,
        CONNECTION_RESPONSE_PACKET_SIZE,
        FRAGMENTED_PAYLOAD_HEADER_SIZE + opts.max_payload_size,
    );
}

pub fn serialize(comptime opts: Options, packet: Packet(opts), out: []u8) SerializeError!usize {
    if (out.len < maxPacketSize(opts)) return error.BufferTooSmall;
    var writer = std.Io.Writer.fixed(out);
    writePacket(opts, packet, &writer) catch |err| switch (err) {
        error.WriteFailed => return error.BufferTooSmall,
        error.InvalidPacket => return error.InvalidPacket,
        error.PayloadTooLarge => return error.PayloadTooLarge,
    };
    return writer.buffered().len;
}

fn writePacket(comptime opts: Options, packet: Packet(opts), writer: *std.Io.Writer) (error{ PayloadTooLarge, InvalidPacket } || std.Io.Writer.Error)!void {
    try writer.writeByte(PROTOCOL_VERSION);
    switch (packet) {
        .ConnectionRequest => |request| switch (request) {
            .Plain => |plain| {
                try writer.writeByte(@backingInt(PacketKind.ConnectionRequestPlain));
                try writer.writeInt(u32, plain.protocol_id, .big);
                try writer.writeInt(u64, plain.client_nonce, .big);
            },
            .Secure => |secure| {
                try writer.writeByte(@backingInt(PacketKind.ConnectionRequestSecure));
                try writer.writeInt(u32, secure.protocol_id, .big);
                try writer.writeInt(u64, secure.client_nonce, .big);
                const token_bytes: *[TokenType(opts).wire_size]u8 = @ptrCast(try writer.writableSlice(TokenType(opts).wire_size));
                secure.token.encode(token_bytes);
            },
        },
        .Challenge => |challenge| {
            try writer.writeByte(@backingInt(PacketKind.Challenge));
            try writer.writeInt(u64, challenge.sequence, .big);
            try writer.writeInt(u64, challenge.expires_at, .big);
            try writer.writeAll(&challenge.token);
        },
        .ConnectionResponse => |response| {
            try writer.writeByte(@backingInt(PacketKind.ConnectionResponse));
            try writer.writeInt(u64, response.sequence, .big);
            try writer.writeAll(&response.token);
        },
        .ConnectionAccepted => try writer.writeByte(@backingInt(PacketKind.ConnectionAccepted)),
        .Disconnect => try writer.writeByte(@backingInt(PacketKind.Disconnect)),
        .Payload => |payload| {
            try validatePayload(opts, payload.header, payload.len);

            var flags: u8 = 0;
            if (payload.header.is_ack) flags |= ACK_FLAG;
            if (payload.header.fragment != null) flags |= FRAGMENT_FLAG;

            try writer.writeByte(@backingInt(PacketKind.Payload));
            try writer.writeByte(flags);
            try writer.writeByte(payload.header.channel_id);
            try writer.writeInt(u16, payload.header.seq, .big);
            try writer.writeInt(u16, payload.len, .big);
            if (payload.header.fragment) |fragment| {
                try writer.writeInt(u16, fragment.msg_id, .big);
                try writer.writeByte(fragment.frag_count);
                try writer.writeByte(fragment.frag_index);
                try writer.writeInt(u16, fragment.total_size, .big);
            }
            try writer.writeAll(payload.body[0..payload.len]);
        },
    }
}

pub fn deserialize(comptime opts: Options, bytes: []const u8) DeserializeError!Packet(opts) {
    if (bytes.len < PACKET_HEADER_SIZE) return error.InvalidPacket;
    var reader = std.Io.Reader.fixed(bytes);
    const packet = readPacket(opts, &reader) catch |err| switch (err) {
        error.EndOfStream, error.ReadFailed => return error.InvalidPacket,
        error.InvalidPacket => return error.InvalidPacket,
        error.UnsupportedVersion => return error.UnsupportedVersion,
    };
    if (reader.seek != bytes.len) return error.InvalidPacket;
    return packet;
}

fn readPacket(comptime opts: Options, reader: *std.Io.Reader) (DeserializeError || std.Io.Reader.Error)!Packet(opts) {
    if (try reader.takeByte() != PROTOCOL_VERSION) return error.UnsupportedVersion;
    const kind = std.enums.fromInt(PacketKind, try reader.takeByte()) orelse return error.InvalidPacket;
    return switch (kind) {
        .ConnectionRequestPlain => .{ .ConnectionRequest = .{ .Plain = .{
            .protocol_id = try reader.takeInt(u32, .big),
            .client_nonce = try reader.takeInt(u64, .big),
        } } },
        .ConnectionRequestSecure => blk: {
            const protocol_id = try reader.takeInt(u32, .big);
            const client_nonce = try reader.takeInt(u64, .big);
            const token_bytes = try reader.takeArray(TokenType(opts).wire_size);
            const token = TokenType(opts).decode(token_bytes) orelse return error.InvalidPacket;
            break :blk .{ .ConnectionRequest = .{ .Secure = .{
                .protocol_id = protocol_id,
                .client_nonce = client_nonce,
                .token = token,
            } } };
        },
        .Challenge => .{ .Challenge = .{
            .sequence = try reader.takeInt(u64, .big),
            .expires_at = try reader.takeInt(u64, .big),
            .token = (try reader.takeArray(@sizeOf(ChallengeToken))).*,
        } },
        .ConnectionResponse => .{ .ConnectionResponse = .{
            .sequence = try reader.takeInt(u64, .big),
            .token = (try reader.takeArray(@sizeOf(ChallengeToken))).*,
        } },
        .ConnectionAccepted => .ConnectionAccepted,
        .Disconnect => .Disconnect,
        .Payload => blk: {
            const flags = try reader.takeByte();
            if (flags & ~(ACK_FLAG | FRAGMENT_FLAG) != 0) return error.InvalidPacket;

            var header: PayloadHeader = .{
                .channel_id = try reader.takeByte(),
                .seq = try reader.takeInt(u16, .big),
                .is_ack = flags & ACK_FLAG != 0,
            };
            const len = try reader.takeInt(u16, .big);
            if (flags & FRAGMENT_FLAG != 0) {
                header.fragment = .{
                    .msg_id = try reader.takeInt(u16, .big),
                    .frag_count = try reader.takeByte(),
                    .frag_index = try reader.takeByte(),
                    .total_size = try reader.takeInt(u16, .big),
                };
            }
            validatePayload(opts, header, len) catch return error.InvalidPacket;
            _ = try reader.take(len);
            // State machines copy user bytes directly from the datagram into the pool.
            break :blk .{ .Payload = .{ .header = header, .len = len, .body = undefined } };
        },
    };
}

fn validatePayload(comptime opts: Options, header: PayloadHeader, len: usize) error{ PayloadTooLarge, InvalidPacket }!void {
    if (len > opts.max_payload_size) return error.PayloadTooLarge;
    if (header.channel_id >= opts.channels.len) return error.InvalidPacket;
    if (header.is_ack and (len != 0 or header.fragment != null)) return error.InvalidPacket;
    if (header.fragment) |fragment| {
        if (!validFragment(opts, header.channel_id, fragment, len)) return error.InvalidPacket;
    }
}
pub fn ConnectionRequest(comptime opts: Options) type {
    comptime validation.options.validate(opts);

    return union(enum) {
        Plain: PlainConnectionRequest,
        Secure: SecureConnectionRequest(opts),
    };
}

pub const PlainConnectionRequest = struct {
    protocol_id: u32,
    client_nonce: u64,
};

pub fn SecureConnectionRequest(comptime opts: Options) type {
    comptime validation.options.validate(opts);

    return struct {
        protocol_id: u32,
        client_nonce: u64,
        token: TokenType(opts),
    };
}

pub const Challenge = struct {
    sequence: u64,
    expires_at: u64,
    token: ChallengeToken,
};

pub const ConnectionResponse = struct {
    sequence: u64,
    token: ChallengeToken,
};

pub fn Payload(comptime opts: Options) type {
    comptime validation.options.validate(opts);

    return struct {
        header: PayloadHeader = .{},
        len: u16,
        /// User data only. Delivery and fragment metadata are stored in header.
        body: [opts.max_payload_size]u8,
    };
}

fn validFragment(comptime opts: Options, channel_id: u8, fragment: FragmentHeader, len: usize) bool {
    const fragment_size = opts.channels[channel_id].fragment_size orelse return false;
    const total_size: usize = fragment.total_size;
    const expected_count = if (total_size == 0) 1 else (total_size - 1) / fragment_size + 1;
    if (fragment.frag_count != expected_count or fragment.frag_index >= fragment.frag_count) return false;
    const offset = @as(usize, fragment.frag_index) * fragment_size;
    return offset <= total_size and len == @min(fragment_size, total_size - offset);
}
