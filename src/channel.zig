const std = @import("std");
const testing = std.testing;

const packet = @import("packet.zig");

pub const ChannelKind = enum {
    Unreliable,
    UnreliableLatest,
    ReliableOrdered,
    ReliableUnordered,
};

pub const FragmentationPolicy = enum {
    // always fragment message even if it fits into one packet
    always,
    // if message is bigger than packet limit and if it fits `total_size`
    // supported by fragment type
    when_needed,
};

/// Per-channel configuration. Use as elements of `Options.channels`.
pub const ChannelConfig = struct {
    kind: ChannelKind,
    /// Unacked send slots per connection (ReliableOrdered / ReliableUnordered send side).
    reliable_buffer: usize = 64,
    /// Future-packet recv buffer depth (ReliableOrdered recv side). 0 = no buffering.
    ordered_recv_window: usize = 32,
    /// Dedup sliding window size (ReliableUnordered recv side).
    unordered_recv_window: usize = 64,
    frag_config: ?FragConfig = null,
};

/// Fragmentation Config for channels
/// UnreliableLatest channels do not support fragmentation.
pub const FragConfig = struct {
    policy: FragmentationPolicy,
    /// Maximum user-data bytes per fragment, at most Options.max_payload_size.
    fragment_size: usize,
    frag_assembly_slots: usize,
};

pub const ChannelInfo = struct {
    config: ChannelConfig,
    /// Compact index within the channel kind (null for plain Unreliable).
    compact_index: ?u8,
    /// Compact index into the fragmented-channel state array; null when fragment_size == null.
    frag_index: ?u8,
};

/// Maps the public wire-level channel_id to a compact index for each channel kind.
/// Example: if channels 1 and 3 are ReliableOrdered, they become ordered indexes 0 and 1.
pub fn ChannelLayout(comptime channels: []const ChannelConfig) type {
    const LayoutData = struct {
        infos: [channels.len]ChannelInfo,
        ordered_count: usize,
        unordered_count: usize,
        latest_count: usize,
        frag_count: usize,
    };

    const data = comptime blk: {
        var infos: [channels.len]ChannelInfo = undefined;
        var ordered_count: u8 = 0;
        var unordered_count: u8 = 0;
        var latest_count: u8 = 0;
        var frag_count: u8 = 0;

        for (channels, 0..) |ch, ch_idx| {
            const fi: ?u8 = if (ch.frag_config != null) fi_blk: {
                const idx = frag_count;
                frag_count += 1;
                break :fi_blk idx;
            } else null;

            switch (ch.kind) {
                .Unreliable => infos[ch_idx] = .{
                    .config = ch,
                    .compact_index = null,
                    .frag_index = fi,
                },
                .UnreliableLatest => {
                    infos[ch_idx] = .{
                        .config = ch,
                        .compact_index = latest_count,
                        .frag_index = fi,
                    };
                    latest_count += 1;
                },
                .ReliableOrdered => {
                    infos[ch_idx] = .{
                        .config = ch,
                        .compact_index = ordered_count,
                        .frag_index = fi,
                    };
                    ordered_count += 1;
                },
                .ReliableUnordered => {
                    infos[ch_idx] = .{
                        .config = ch,
                        .compact_index = unordered_count,
                        .frag_index = fi,
                    };
                    unordered_count += 1;
                },
            }
        }

        break :blk LayoutData{
            .infos = infos,
            .ordered_count = ordered_count,
            .unordered_count = unordered_count,
            .latest_count = latest_count,
            .frag_count = frag_count,
        };
    };

    return struct {
        pub const channel_count = channels.len;
        pub const infos = data.infos;
        pub const ordered_count = data.ordered_count;
        pub const unordered_count = data.unordered_count;
        pub const latest_count = data.latest_count;
        /// Number of channels with `fragment_size != null`.
        pub const frag_count = data.frag_count;

        pub fn kind(channel_id: u8) ChannelKind {
            return infos[channel_id].config.kind;
        }

        pub fn config(channel_id: u8) ChannelConfig {
            return infos[channel_id].config;
        }

        pub fn compactIndex(channel_id: u8) ?u8 {
            return infos[channel_id].compact_index;
        }

        pub fn orderedIndex(channel_id: u8) ?u8 {
            if (ordered_count == 0) return null;
            if (kind(channel_id) != .ReliableOrdered) return null;
            return compactIndex(channel_id);
        }

        pub fn unorderedIndex(channel_id: u8) ?u8 {
            if (unordered_count == 0) return null;
            if (kind(channel_id) != .ReliableUnordered) return null;
            return compactIndex(channel_id);
        }

        pub fn latestIndex(channel_id: u8) ?u8 {
            if (latest_count == 0) return null;
            if (kind(channel_id) != .UnreliableLatest) return null;
            return compactIndex(channel_id);
        }

        /// Returns the compact fragmentation index for `channel_id`, or null if the
        /// channel does not have `frag_config` set.
        pub fn fragIndex(channel_id: u8) ?u8 {
            return infos[channel_id].frag_index;
        }
    };
}

/// Per-connection smoothed RTT state for adaptive retransmission.
pub const RttState = struct {
    srtt: u64 = 0,
    initialized: bool = false,

    /// Feed an RTT sample (now - sent_at) from an ACK.
    pub fn update(self: *RttState, rtt_sample: u64) void {
        if (!self.initialized) {
            self.srtt = rtt_sample;
            self.initialized = true;
        } else {
            // EWMA: srtt = srtt * 7/8 + sample * 1/8
            self.srtt = (self.srtt * 7 + rtt_sample) / 8;
        }
    }

    /// Compute retransmission timeout: max(srtt * 2, min_rto).
    pub fn rto(self: *const RttState, min_rto: u64) u64 {
        if (!self.initialized) return min_rto;
        return @max(self.srtt * 2, min_rto);
    }
};

/// Per-channel receive state for UnreliableLatest: drop packets older than last received.
pub const UnreliableLatestState = struct {
    last_seq: u16 = 0,
    initialized: bool = false,

    pub fn accept(self: *UnreliableLatestState, seq: u16) bool {
        if (!self.initialized) {
            self.initialized = true;
            self.last_seq = seq;
            return true;
        }
        if (seqGreater(seq, self.last_seq)) {
            self.last_seq = seq;
            return true;
        }
        return false;
    }
};

/// Per-channel state for Reliable: buffer unACKed outgoing messages and retransmit.
pub fn ReliableState(comptime buf_size: usize, comptime data_cap: usize) type {
    return struct {
        const Self = @This();

        pub const Entry = struct {
            seq: u16 = 0,
            fragment: ?packet.FragmentHeader = null,
            data: [data_cap]u8 = undefined,
            len: usize = 0,
            sent_at: u64 = 0,
            active: bool = false,
        };

        send_seq: u16 = 0,
        entries: [buf_size]Entry = @splat(.{}),

        pub fn canPush(self: *const Self, count: usize) bool {
            if (count > buf_size or count > std.math.maxInt(u16)) return false;
            for (1..count + 1) |offset| {
                const seq = self.send_seq +% @as(u16, @intCast(offset));
                const index = @as(usize, seq) % buf_size;
                if (self.entries[index].active) return false;
                // Wrapping the sequence can revisit an index in non-power-of-two buffers.
                if (seq < self.send_seq) {
                    for (1..offset) |previous| {
                        const previous_seq = self.send_seq +% @as(u16, @intCast(previous));
                        if (@as(usize, previous_seq) % buf_size == index) return false;
                    }
                }
            }
            return true;
        }

        pub fn isNextNotEmpty(self: *const Self) bool {
            const next_seq = self.send_seq +% 1;
            const idx = @as(usize, next_seq) % buf_size;
            return self.entries[idx].active;
        }

        /// Enqueue a message for reliable delivery. Returns the assigned sequence number,
        /// or null if the send buffer is full (the slot for the next seq is still unACKed).
        pub fn push(self: *Self, data: []const u8, now: u64) ?u16 {
            const next_seq = self.send_seq +% 1;
            const idx = @as(usize, next_seq) % buf_size;
            if (self.entries[idx].active) return null; // buffer full
            self.send_seq = next_seq;
            const copy_len = @min(data.len, data_cap);
            self.entries[idx] = .{
                .seq = next_seq,
                .len = copy_len,
                .sent_at = now,
                .active = true,
            };
            @memcpy(self.entries[idx].data[0..copy_len], data[0..copy_len]);
            return next_seq;
        }

        /// Retain fragment metadata alongside the bytes needed for retransmission.
        pub fn pushFragment(self: *Self, data: []const u8, now: u64, fragment: packet.FragmentHeader) ?u16 {
            const seq = self.push(data, now) orelse return null;
            self.entries[@as(usize, seq) % buf_size].fragment = fragment;
            return seq;
        }

        /// Mark a sequence number as acknowledged; frees the slot.
        /// Returns the entry's sent_at timestamp for RTT measurement, or null if not found.
        pub fn ack(self: *Self, seq: u16) ?u64 {
            const idx = @as(usize, seq) % buf_size;
            if (self.entries[idx].active and self.entries[idx].seq == seq) {
                const sent_at = self.entries[idx].sent_at;
                self.entries[idx].active = false;
                return sent_at;
            }
            return null;
        }

        /// Mark an entry as re-sent (update sent_at) so the resend timer resets.
        pub fn markResent(self: *Self, seq: u16, now: u64) void {
            const idx = @as(usize, seq) % buf_size;
            if (self.entries[idx].active and self.entries[idx].seq == seq) {
                self.entries[idx].sent_at = now;
            }
        }
    };
}

pub const ReliableOrderedRecvAction = enum {
    deliver,
    // Packet was accepted or recognized, but nothing should reach the app yet.
    ack_only,
    // Packet is outside the receive window and is ignored.
    drop,
};

pub fn ReliableOrderedRecvState(
    comptime recv_window_size: usize,
    comptime max_packet_size: usize,
) type {
    return struct {
        const Self = @This();

        pub const FutureEntry = struct {
            seq: u16 = 0,
            fragment: ?packet.FragmentHeader = null,
            len: usize = 0,
            active: bool = false,
            data: [max_packet_size]u8 = undefined,
        };

        next_seq: u16 = 1,
        futures: ?*[recv_window_size]FutureEntry = null,

        pub fn receive(
            self: *Self,
            allocator: std.mem.Allocator,
            seq: u16,
            body: []const u8,
        ) !ReliableOrderedRecvAction {
            return self.receiveFragment(allocator, seq, body, null);
        }

        pub fn receiveFragment(
            self: *Self,
            allocator: std.mem.Allocator,
            seq: u16,
            body: []const u8,
            fragment: ?packet.FragmentHeader,
        ) !ReliableOrderedRecvAction {
            if (seq == self.next_seq) {
                self.next_seq +%= 1;
                return .deliver;
            }

            if (!seqGreater(seq, self.next_seq)) return .ack_only;
            if (comptime recv_window_size == 0) return .drop;

            const ahead = seq -% self.next_seq;
            if (ahead > recv_window_size) return .drop;

            const futures = try self.getOrCreateFutures(allocator);
            const idx = @as(usize, seq) % recv_window_size;
            const entry = &futures[idx];
            if (entry.active and entry.seq == seq) return .ack_only;

            // Store the packet until the missing gap before it is received.
            const copy_len = @min(body.len, max_packet_size);
            entry.* = .{
                .seq = seq,
                .len = copy_len,
                .active = true,
                .fragment = fragment,
            };
            @memcpy(entry.data[0..copy_len], body[0..copy_len]);
            return .ack_only;
        }

        // Called on .deliver in transport
        // pop buffered packets
        pub fn popReady(self: *Self, out: []u8) ?usize {
            const ready = self.peekReady() orelse return null;
            const len = @min(ready.len, out.len);
            @memcpy(out[0..len], ready[0..len]);
            self.consumeReady();
            return len;
        }

        pub fn peekReady(self: *Self) ?[]const u8 {
            if (comptime recv_window_size == 0) return null;
            const futures = self.futures orelse return null;

            // After a gap is filled, release any buffered packets that are now contiguous.
            const idx = @as(usize, self.next_seq) % recv_window_size;
            const entry = &futures[idx];
            if (!entry.active or entry.seq != self.next_seq) return null;
            return entry.data[0..entry.len];
        }

        pub fn readyFragment(self: *Self) ?packet.FragmentHeader {
            if (comptime recv_window_size == 0) return null;
            _ = self.peekReady() orelse return null;
            const futures = self.futures orelse return null;
            return futures[@as(usize, self.next_seq) % recv_window_size].fragment;
        }

        pub fn consumeReady(self: *Self) void {
            if (comptime recv_window_size == 0) return;
            const futures = self.futures orelse return;
            const idx = @as(usize, self.next_seq) % recv_window_size;
            const entry = &futures[idx];
            if (!entry.active or entry.seq != self.next_seq) return;

            entry.active = false;
            self.next_seq +%= 1;
        }

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            if (self.futures) |futures| {
                allocator.destroy(futures);
                self.futures = null;
            }
        }

        fn getOrCreateFutures(self: *Self, allocator: std.mem.Allocator) !*[recv_window_size]FutureEntry {
            if (self.futures) |futures| return futures;

            const futures = try allocator.create([recv_window_size]FutureEntry);
            futures.* = @as([recv_window_size]FutureEntry, @splat(.{}));
            self.futures = futures;
            return futures;
        }
    };
}

pub const ReliableUnorderedRecvAction = enum {
    deliver,
    duplicate,
    stale,
};

pub fn ReliableUnorderedRecvState(comptime window_size: usize) type {
    comptime {
        if (window_size == 0)
            @compileError("ReliableUnorderedRecvState window_size must be greater than zero");
    }

    return struct {
        const Self = @This();

        initialized: bool = false,
        newest_seq: u16 = 0,
        seen: [window_size]?u16 = @splat(null),

        pub fn classify(self: *Self, seq: u16) ReliableUnorderedRecvAction {
            if (!self.initialized) {
                self.initialized = true;
                self.newest_seq = seq;
                self.seen[indexFor(seq)] = seq;
                return .deliver;
            }

            if (!seqGreater(seq, self.newest_seq)) {
                const behind = self.newest_seq -% seq;
                if (behind >= window_size) return .stale;
            }

            const idx = indexFor(seq);
            if (self.seen[idx]) |seen_seq| {
                if (seen_seq == seq) return .duplicate;
            }

            self.seen[idx] = seq;
            if (seqGreater(seq, self.newest_seq)) self.newest_seq = seq;
            return .deliver;
        }

        fn indexFor(seq: u16) usize {
            return @as(usize, seq) % window_size;
        }
    };
}

/// Sequence comparison with 16-bit wrapping (half-space rule).
/// Returns true if `a` is strictly after `b` in sequence space.
pub fn seqGreater(a: u16, b: u16) bool {
    return a != b and (a -% b) < 0x8000;
}

// --- Tests ---

test "seqGreater basic" {
    try testing.expect(seqGreater(1, 0));
    try testing.expect(seqGreater(100, 50));
    try testing.expect(!seqGreater(0, 1));
    try testing.expect(!seqGreater(1, 1));
    try testing.expect(!seqGreater(50, 100));
    std.debug.print("\n  PASS: seqGreater basic\n", .{});
}

test "seqGreater wrapping" {
    // 0 is one ahead of 65535 in wrapping arithmetic
    try testing.expect(seqGreater(0, 65535));
    // 1 is two ahead of 65535
    try testing.expect(seqGreater(1, 65535));
    // 65535 is one ahead of 65534
    try testing.expect(seqGreater(65535, 65534));
    // Exactly half-space away: ambiguous, treated as not greater
    try testing.expect(!seqGreater(32768, 0));
    std.debug.print("\n  PASS: seqGreater wrapping\n", .{});
}

test "UnreliableLatestState" {
    var state: UnreliableLatestState = .{};
    // First packet always accepted
    try testing.expect(state.accept(5));
    // Newer accepted
    try testing.expect(state.accept(6));
    try testing.expect(state.accept(10));
    // Older or equal rejected
    try testing.expect(!state.accept(10));
    try testing.expect(!state.accept(9));
    try testing.expect(!state.accept(1));
    // Newer again accepted
    try testing.expect(state.accept(11));
    std.debug.print("\n  PASS: UnreliableLatestState\n", .{});
}

test "UnreliableLatestState wrapping" {
    var state: UnreliableLatestState = .{};
    _ = state.accept(65534);
    _ = state.accept(65535);
    // 0 wraps past 65535
    try testing.expect(state.accept(0));
    try testing.expect(state.accept(1));
    try testing.expect(!state.accept(65535));
    std.debug.print("\n  PASS: UnreliableLatestState wrapping\n", .{});
}

test "ReliableState batch admission handles sequence wrap and occupied slots" {
    var uneven: ReliableState(3, 16) = .{};
    uneven.send_seq = 65534;
    try testing.expect(uneven.canPush(1));
    try testing.expect(!uneven.canPush(2));

    var state: ReliableState(4, 16) = .{};
    state.send_seq = 65534;
    try testing.expect(state.canPush(4));
    state.entries[0].active = true;
    try testing.expect(!state.canPush(2));
    try testing.expect(!state.canPush(5));
}

test "ReliableState push and ack" {
    const S = ReliableState(4, 32);
    var state: S = .{};

    const data = [_]u8{ 1, 2, 3 };
    const seq1 = state.push(&data, 0).?;
    try testing.expectEqual(@as(u16, 1), seq1);

    const seq2 = state.push(&data, 0).?;
    try testing.expectEqual(@as(u16, 2), seq2);

    _ = state.ack(seq1);

    // Slot freed, can push again
    const seq3 = state.push(&data, 0).?;
    try testing.expectEqual(@as(u16, 3), seq3);
    std.debug.print("\n  PASS: ReliableState push and ack\n", .{});
}

test "ReliableState buffer full" {
    const S = ReliableState(2, 8);
    var state: S = .{};
    const data = [_]u8{0};
    _ = state.push(&data, 0).?;
    _ = state.push(&data, 0).?;
    try testing.expect(state.push(&data, 0) == null);
    std.debug.print("\n  PASS: ReliableState buffer full\n", .{});
}

test "ReliableOrderedRecvState buffers future packet and flushes after gap closes" {
    const S = ReliableOrderedRecvState(4, 16);
    var state: S = .{};
    defer state.deinit(testing.allocator);

    const body1 = [_]u8{ 1, 10, 11, 12 };
    const body2 = [_]u8{ 2, 20, 21, 22 };
    var out: [16]u8 = undefined;

    try testing.expectEqual(
        ReliableOrderedRecvAction.ack_only,
        try state.receive(testing.allocator, 2, body2[0..]),
    );
    try testing.expect(state.popReady(out[0..]) == null);

    try testing.expectEqual(
        ReliableOrderedRecvAction.deliver,
        try state.receive(testing.allocator, 1, body1[0..]),
    );

    const len = state.popReady(out[0..]).?;
    try testing.expectEqual(body2.len, len);
    try testing.expectEqualSlices(u8, body2[0..], out[0..len]);
    try testing.expect(state.popReady(out[0..]) == null);
    std.debug.print("\n  PASS: ReliableOrderedRecvState buffers future packet and flushes after gap closes\n", .{});
}

test "ReliableOrderedRecvState drops future packet when recv window disabled" {
    const S = ReliableOrderedRecvState(0, 8);
    var state: S = .{};
    defer state.deinit(testing.allocator);

    const body = [_]u8{ 9, 9, 9 };
    try testing.expectEqual(
        ReliableOrderedRecvAction.drop,
        try state.receive(testing.allocator, 2, body[0..]),
    );
    std.debug.print("\n  PASS: ReliableOrderedRecvState drops future packet when recv window disabled\n", .{});
}

test "ReliableUnorderedRecvState deduplicates within window" {
    const S = ReliableUnorderedRecvState(8);
    var state: S = .{};

    try testing.expectEqual(ReliableUnorderedRecvAction.deliver, state.classify(10));
    try testing.expectEqual(ReliableUnorderedRecvAction.deliver, state.classify(12));
    try testing.expectEqual(ReliableUnorderedRecvAction.deliver, state.classify(11));
    try testing.expectEqual(ReliableUnorderedRecvAction.duplicate, state.classify(12));

    std.debug.print("\n  PASS: ReliableUnorderedRecvState deduplicates within window\n", .{});
}

test "ReliableUnorderedRecvState drops stale packets outside window" {
    const S = ReliableUnorderedRecvState(4);
    var state: S = .{};

    try testing.expectEqual(ReliableUnorderedRecvAction.deliver, state.classify(10));
    try testing.expectEqual(ReliableUnorderedRecvAction.deliver, state.classify(11));
    try testing.expectEqual(ReliableUnorderedRecvAction.deliver, state.classify(12));
    try testing.expectEqual(ReliableUnorderedRecvAction.deliver, state.classify(13));
    try testing.expectEqual(ReliableUnorderedRecvAction.deliver, state.classify(14));
    try testing.expectEqual(ReliableUnorderedRecvAction.stale, state.classify(10));

    std.debug.print("\n  PASS: ReliableUnorderedRecvState drops stale packets outside window\n", .{});
}
