const std = @import("std");
const SocketError = @import("../transport/error.zig").SocketError;
const RecvResult = @import("../transport/socket.zig").RecvResult;
const Address = @import("../root.zig").Address;

pub const UdpSocket = struct {
    pub const Error = SocketError;
    io_threaded: std.Io.Threaded,
    socket: std.Io.net.Socket,

    pub fn open(addr: Address) SocketError!UdpSocket {
        var io_threaded: std.Io.Threaded = .init(std.heap.page_allocator, .{});
        errdefer io_threaded.deinit();

        const socket = try addr.bind(io_threaded.io(), .{
            .mode = .dgram,
        });

        return .{
            .io_threaded = io_threaded,
            .socket = socket,
        };
    }

    pub fn close(self: *UdpSocket) void {
        self.socket.close(self.io_threaded.io());
        self.io_threaded.deinit();
    }

    pub fn recvfrom(self: *UdpSocket, buf: []u8) ?RecvResult {
        var message = std.Io.net.IncomingMessage.init;
        const maybe_err, const count = self.socket.receiveManyTimeout(
            self.io_threaded.io(),
            (&message)[0..1],
            buf,
            .{},
            .{ .duration = .{ .clock = .awake, .raw = .fromNanoseconds(0) } },
        );
        if (maybe_err != null or count == 0) return null;

        return .{
            .addr = message.from,
            .len = message.data.len,
        };
    }

    pub fn sendto(self: *UdpSocket, addr: Address, data: []const u8) void {
        self.socket.send(self.io_threaded.io(), &addr, data) catch {};
    }
};
