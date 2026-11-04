const std = @import("std");

pub const SECRET_KEY_SIZE = 32;
pub const CHALLENGE_KEY_SIZE = 32;
pub const Address = std.Io.net.IpAddress;

pub const handshake = @import("handshake.zig");

pub const ChannelKind = @import("channel.zig").ChannelKind;
pub const ChannelConfig = @import("channel.zig").ChannelConfig;

/// Single comptime config shared by both Server and Client.
/// Both sides must use identical options — the wire format depends on it.
pub const Options = struct {
    max_clients: usize = 1024,
    /// Maximum number of clients in the handshake (pending) + recycled slot pool.
    /// Must be a power of two. Defaults to `max_clients * 2` when null.
    max_pending_clients: ?usize = null,
    nonce_window: usize = 256,
    outgoing_queue_size: usize = 256,
    events_queue_size: usize = 256,
    /// Maximum number of client public addresses carried by the built-in token.
    /// Must fit in a u8 because the wire format stores the count in one byte.
    max_token_addresses: usize = 8,
    user_data_size: usize = 256,
    max_payload_size: usize = 1024,
    /// User-provided ConnectToken type. `void` = use zanoza's built-in default.
    ConnectToken: type = void,
    /// Per-channel configuration. Index = channel_id. At least one channel required.
    /// Use `ChannelConfig{ .kind = .ReliableOrdered, .reliable_buffer = 32, ... }`.
    channels: []const ChannelConfig = &.{.{ .kind = .Unreliable }},
    /// Nanoseconds before an unACKed reliable message is resent.
    reliable_resend_ns: u64 = 100 * std.time.ns_per_ms,
    /// Capacity of the incoming message queue in TransportServer/TransportClient.
    /// Sized separately from events_queue_size because messages arrive far more often.
    messages_queue_size: usize = 256,
};

pub const Client = @import("client/client.zig").Client;
pub const ClientConfig = @import("client/config.zig").ClientConfig;
pub const ClientError = @import("client/error.zig").ClientError;

pub const Server = @import("server/server.zig").Server;
pub const ServerConfig = @import("server/config.zig").ServerConfig;
pub const ServerError = @import("server/error.zig").ServerError;

pub const TransportServer = @import("transport/server.zig").TransportServer;
pub const TransportClient = @import("transport/client.zig").TransportClient;
pub const UdpSocket = @import("transport/udp.zig").UdpSocket;
pub const LoopbackSocket = @import("transport/loopback.zig").LoopbackSocket;
pub const PayloadPool = @import("payload_pool.zig").PayloadPool;

comptime {
    _ = @import("channel.zig");
    _ = @import("payload_pool.zig");
    _ = @import("tests.zig");
}
