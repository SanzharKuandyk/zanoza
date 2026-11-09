const std = @import("std");
const ClientError = @import("../client/error.zig").ClientError;
const ServerError = @import("../server/error.zig").ServerError;

/// Channel delivery and transport storage failures shared by both transports.
pub const TransportError = error{
    /// The channel ID is absent from Options.channels.
    InvalidChannel,
    /// The complete message exceeds the channel or fragment wire limits.
    MessageTooLarge,
    /// The next reliable send slots are still awaiting acknowledgement.
    ReliableBufferFull,
} || std.mem.Allocator.Error;

/// Client transport operations also expose the raw client state-machine errors.
pub const TransportClientError = ClientError || TransportError;
/// Server transport operations also expose the raw server state-machine errors.
pub const TransportServerError = ServerError || TransportError;

/// Built-in UDP startup and bind failures across supported platforms.
pub const SocketError = error{
    SocketInitFailed,
    SocketOpenFailed,
    SocketNonblockingFailed,
    SocketBindFailed,
} || std.Io.net.IpAddress.BindError;
