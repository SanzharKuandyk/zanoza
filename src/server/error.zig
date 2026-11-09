const std = @import("std");

/// Server state-machine protocol, capacity, and allocation failures.
pub const ServerError = error{
    Expired,
    /// A bounded queue or payload pool is exhausted.
    IoError,
    ServerFull,
    InvalidPacket,
    UnknownClient,
    InvalidProtocolId,
    PayloadTooLarge,
} || std.mem.Allocator.Error;
