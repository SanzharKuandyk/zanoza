const std = @import("std");
const RingQueue = @import("ring_buffer.zig").RingQueue;

pub fn RecentNonces(comptime Window: usize) type {
    return struct {
        const Self = @This();

        order: RingQueue(u64, Window),
        seen: std.AutoHashMap(u64, void),

        pub fn init(allocator: std.mem.Allocator) std.mem.Allocator.Error!Self {
            var seen = std.AutoHashMap(u64, void).init(allocator);
            try seen.ensureTotalCapacity(Window);
            return .{
                .order = .{},
                .seen = seen,
            };
        }

        pub fn deinit(self: *Self) void {
            self.seen.deinit();
        }

        pub fn contains(self: *const Self, nonce: u64) bool {
            return self.seen.contains(nonce);
        }

        /// Returns false if nonce was already seen (replay), true if inserted.
        pub fn insert(self: *Self, nonce: u64) bool {
            if (self.seen.contains(nonce)) return false;

            if (self.order.isFull()) {
                const old = self.order.popFront().?;
                if (!self.seen.remove(old)) unreachable;
            }

            if (!self.order.pushBack(nonce)) unreachable;
            self.seen.putAssumeCapacityNoClobber(nonce, {});
            return true;
        }
    };
}
