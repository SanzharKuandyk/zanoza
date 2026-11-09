const std = @import("std");
const SocketError = @import("../transport/error.zig").SocketError;
const RecvResult = @import("../transport/socket.zig").RecvResult;
const Address = @import("../root.zig").Address;

const Threaded = std.Io.Threaded;
const ws2_32 = std.os.windows.ws2_32;

const c = struct {
    extern "ws2_32" fn socket(af: c_int, kind: c_int, protocol: c_int) callconv(.winapi) Handle;
    extern "ws2_32" fn bind(sockfd: Handle, addr: *const ws2_32.sockaddr, addrlen: ws2_32.socklen_t) callconv(.winapi) c_int;
    extern "ws2_32" fn closesocket(sockfd: Handle) callconv(.winapi) c_int;
    extern "ws2_32" fn ioctlsocket(sockfd: Handle, cmd: i32, argp: *u32) callconv(.winapi) c_int;
    extern "ws2_32" fn sendto(
        sockfd: Handle,
        buf: [*]const u8,
        len: c_int,
        flags: c_int,
        dest_addr: *const ws2_32.sockaddr,
        addrlen: ws2_32.socklen_t,
    ) callconv(.winapi) c_int;
    extern "ws2_32" fn recvfrom(
        sockfd: Handle,
        buf: [*]u8,
        len: c_int,
        flags: c_int,
        src_addr: *ws2_32.sockaddr,
        addrlen: *ws2_32.socklen_t,
    ) callconv(.winapi) c_int;
};

const Handle = usize;
const INVALID_SOCKET = std.math.maxInt(Handle);
const SOCKET_ERROR = -1;
const WSAEWOULDBLOCK = 10035;
const FIONBIO: i32 = @bitCast(@as(u32, 0x8004667e));

const WSAData = extern struct {
    version: u16,
    high_version: u16,
    description: [257]u8,
    system_status: [129]u8,
    max_sockets: u16,
    max_udp_dg: u16,
    vendor_info: ?[*:0]u8,
};

extern "ws2_32" fn WSAStartup(version_requested: u16, data: *WSAData) callconv(.winapi) c_int;
extern "ws2_32" fn WSACleanup() callconv(.winapi) c_int;
extern "ws2_32" fn WSAGetLastError() callconv(.winapi) c_int;

pub const UdpSocket = struct {
    pub const Error = SocketError;
    handle: Handle,

    pub fn open(addr: Address) SocketError!UdpSocket {
        var wsa_data: WSAData = undefined;
        if (WSAStartup(0x0202, &wsa_data) != 0) return error.SocketInitFailed;
        errdefer _ = WSACleanup();

        const family: c_int = switch (addr) {
            .ip4 => ws2_32.AF.INET,
            .ip6 => ws2_32.AF.INET6,
        };
        const handle = c.socket(family, ws2_32.SOCK.DGRAM, ws2_32.IPPROTO.UDP);
        if (handle == INVALID_SOCKET) return error.SocketOpenFailed;
        errdefer _ = c.closesocket(handle);

        var nonblocking: u32 = 1;
        if (c.ioctlsocket(handle, FIONBIO, &nonblocking) == SOCKET_ERROR)
            return error.SocketNonblockingFailed;

        var storage: Threaded.PosixAddress = undefined;
        const addrlen: ws2_32.socklen_t = @intCast(Threaded.addressToPosix(&addr, &storage));
        if (c.bind(handle, @ptrCast(&storage.any), addrlen) == SOCKET_ERROR)
            return error.SocketBindFailed;

        return .{ .handle = handle };
    }

    pub fn close(self: *UdpSocket) void {
        _ = c.closesocket(self.handle);
        _ = WSACleanup();
    }

    pub fn recvfrom(self: *UdpSocket, buf: []u8) ?RecvResult {
        var storage: Threaded.PosixAddress = undefined;
        var addrlen: ws2_32.socklen_t = @sizeOf(Threaded.PosixAddress);
        const len = c.recvfrom(
            self.handle,
            buf.ptr,
            std.math.cast(c_int, buf.len) orelse return null,
            0,
            @ptrCast(&storage.any),
            &addrlen,
        );
        if (len == SOCKET_ERROR) {
            if (WSAGetLastError() == WSAEWOULDBLOCK) return null;
            return null;
        }

        return .{
            .addr = Threaded.addressFromPosix(&storage),
            .len = @intCast(len),
        };
    }

    pub fn sendto(self: *UdpSocket, addr: Address, data: []const u8) void {
        var storage: Threaded.PosixAddress = undefined;
        const addrlen: ws2_32.socklen_t = @intCast(Threaded.addressToPosix(&addr, &storage));
        _ = c.sendto(
            self.handle,
            data.ptr,
            std.math.cast(c_int, data.len) orelse return,
            0,
            @ptrCast(&storage.any),
            addrlen,
        );
    }
};
