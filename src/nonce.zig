const std = @import("std");
const RingQueue = @import("ring_buffer.zig").RingQueue;

pub fn RecentNonces(comptime Window: usize) type {
    return struct {
        const Self = @This();

        order: RingQueue(u64, Window),
        seen: std.AutoHashMap(u64, void),

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{
                .order = .{},
                .seen = std.AutoHashMap(u64, void).init(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            self.seen.deinit();
        }

        pub fn contains(self: *const Self, nonce: u64) bool {
            return self.seen.contains(nonce);
        }

        /// Returns false if nonce was already seen (replay), true if inserted.
        pub fn insert(self: *Self, nonce: u64) std.mem.Allocator.Error!bool {
            if (self.seen.contains(nonce)) return false;

            if (self.order.isFull()) {
                const old = self.order.popFront().?;
                _ = self.seen.remove(old);
            }

            _ = self.order.pushBack(nonce);
            try self.seen.put(nonce, {});
            return true;
        }
    };
}
