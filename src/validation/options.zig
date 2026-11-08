const std = @import("std");
const root = @import("../root.zig");

fn requirePowerOfTwo(comptime name: []const u8, comptime value: usize) void {
    if (value == 0 or (value & (value - 1)) != 0)
        @compileError(name ++ " must be a power of two and greater than zero");
}

pub fn validate(comptime opts: root.Options) void {
    comptime {
        if (opts.max_clients == 0)
            @compileError("Options.max_clients must be greater than zero");

        // The server reuses this capacity for the recycled slot queue, which is a RingQueue.
        const pending_cap = opts.max_pending_clients orelse blk: {
            const mul = @mulWithOverflow(opts.max_clients, 2);
            if (mul[1] != 0)
                @compileError("Options.max_clients is too large to derive default max_pending_clients");
            break :blk mul[0];
        };
        requirePowerOfTwo("Options.max_pending_clients", pending_cap);
        requirePowerOfTwo("Options.nonce_window", opts.nonce_window);
        requirePowerOfTwo("Options.outgoing_queue_size", opts.outgoing_queue_size);
        requirePowerOfTwo("Options.events_queue_size", opts.events_queue_size);
        requirePowerOfTwo("Options.messages_queue_size", opts.messages_queue_size);

        if (opts.ConnectToken == void) {
            if (opts.max_token_addresses == 0)
                @compileError("Options.max_token_addresses must be greater than zero");
            if (opts.max_token_addresses > std.math.maxInt(u8))
                @compileError("Options.max_token_addresses must fit in u8");
        }

        if (opts.max_payload_size == 0)
            @compileError("Options.max_payload_size must be greater than zero");
        // Payload packets encode length in a u16 on the wire.
        if (opts.max_payload_size > std.math.maxInt(u16))
            @compileError("Options.max_payload_size must fit in the packet u16 length field");

        if (opts.channels.len == 0)
            @compileError("Options.channels must have at least one entry");
        if (opts.channels.len > 128)
            @compileError("Options.channels supports at most 128 entries");

        for (opts.channels) |ch| {
            switch (ch.kind) {
                .ReliableOrdered, .ReliableUnordered => {
                    if (ch.reliable_buffer == 0)
                        @compileError("ChannelConfig.reliable_buffer must be greater than zero for reliable channels");
                },
                else => {},
            }
            if (ch.fragment_size) |frag_sz| {
                if (ch.kind == .UnreliableLatest)
                    @compileError("UnreliableLatest channels do not support fragmentation; use Unreliable instead");
                if (frag_sz == 0)
                    @compileError("ChannelConfig.fragment_size must be greater than zero");
                if (frag_sz > opts.max_payload_size)
                    @compileError("ChannelConfig.fragment_size exceeds max_payload_size");
            }
        }
    }
}
