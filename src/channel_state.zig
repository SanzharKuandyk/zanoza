//! Channel storage generated from each channel's comptime configuration.

const std = @import("std");
const channel = @import("channel.zig");

/// Clients embed send buffers. Servers allocate them on first use.
pub const SendStorage = enum { embedded, lazy };

pub const AssemblyBuf = struct {
    msg_id: u16 = 0,
    frag_count: u8 = 0,
    frags_received: u8 = 0,
    total_size: u16 = 0,
    data: ?[]u8 = null,
    received_mask: [32]u8 = @splat(0),
    active: bool = false,

    pub fn hasFrag(self: *const @This(), fi: u8) bool {
        const bit: u3 = @intCast(fi % 8);
        return (self.received_mask[fi / 8] >> bit) & 1 != 0;
    }

    pub fn markFrag(self: *@This(), fi: u8) void {
        const bit: u3 = @intCast(fi % 8);
        self.received_mask[fi / 8] |= @as(u8, 1) << bit;
    }

    pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        if (self.data) |d| allocator.free(d);
        self.* = .{};
    }
};

fn FragmentState(comptime slot_count: usize) type {
    return struct {
        next_msg_id: u16 = 0,
        slots: [slot_count]AssemblyBuf = @splat(.{}),

        fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            for (&self.slots) |*slot| slot.deinit(allocator);
            self.* = .{};
        }
    };
}

/// Each channel receives its own concrete send, receive, and assembly types.
pub fn ChannelState(comptime config: channel.ChannelConfig, comptime payload_size: usize, comptime storage: SendStorage) type {
    const reliable = config.kind == .ReliableOrdered or config.kind == .ReliableUnordered;
    return struct {
        pub const Send = if (reliable) channel.ReliableState(config.reliable_buffer, payload_size) else void;
        pub const Recv = switch (config.kind) {
            .Unreliable => void,
            .UnreliableLatest => channel.UnreliableLatestState,
            .ReliableOrdered => channel.ReliableOrderedRecvState(config.ordered_recv_window, payload_size),
            .ReliableUnordered => channel.ReliableUnorderedRecvState(config.unordered_recv_window),
        };
        pub const Fragments = if (config.frag_config) |cfg| FragmentState(cfg.frag_assembly_slots) else void;

        const StoredSend = if (reliable and storage == .lazy) ?*Send else Send;

        send: StoredSend = if (!reliable) {} else if (storage == .lazy) null else .{},
        recv: Recv = if (config.kind == .Unreliable) {} else .{},
        send_seq: if (config.kind == .UnreliableLatest) u16 else void = if (config.kind == .UnreliableLatest) 0 else {},
        fragments: Fragments = if (config.frag_config != null) .{} else {},

        pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            if (reliable and storage == .lazy) {
                if (self.send) |send| allocator.destroy(send);
            }
            if (config.kind == .ReliableOrdered) self.recv.deinit(allocator);
            if (config.frag_config != null) self.fragments.deinit(allocator);
            self.* = .{};
        }
    };
}

pub fn States(comptime configs: []const channel.ChannelConfig, comptime payload_size: usize, comptime storage: SendStorage) type {
    var types: [configs.len]type = undefined;
    for (configs, 0..) |config, index| {
        types[index] = ChannelState(config, payload_size, storage);
    }
    return @Tuple(&types);
}

pub fn initStates(comptime configs: []const channel.ChannelConfig, comptime payload_size: usize, comptime storage: SendStorage) States(configs, payload_size, storage) {
    var states: States(configs, payload_size, storage) = undefined;
    inline for (configs, 0..) |_, index| states[index] = .{};
    return states;
}
