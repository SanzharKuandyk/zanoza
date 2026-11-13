const std = @import("std");
const Options = @import("../root.zig").Options;
const Address = @import("../root.zig").Address;

pub fn Connection(comptime opts: Options) type {
    return struct {
        cid: u64,
        sequence: u64,
        addr: Address,
        last_recv: u64,
        last_send: u64,
        user_data: ?[opts.user_data_size]u8,
    };
}

pub fn PendingConnection(comptime opts: Options) type {
    return struct {
        slot: usize,
        client_nonce: u64,
        sequence: u64,
        expires_at: u64,
        user_data: ?[opts.user_data_size]u8,
    };
}
