/// In-memory loopback socket pair for testing.
///
/// `LoopbackSocket.Pair` holds two fixed-size byte queues; `serverSocket` and
/// `clientSocket` return `LoopbackSocket` values whose send/recv pointers are
/// wired so that what one side sends the other side receives.
///
/// The pair must remain alive (on the stack) for the lifetime of both sockets
/// because the sockets hold interior pointers into it.
///
/// Usage:
///
///   var pair: LoopbackSocket.Pair = .{};
///   var srv = try TransportServer(opts, LoopbackSocket)
///       .initWithSocket(allocator, cfg, pair.serverSocket(srv_addr));
///   var cli = try TransportClient(opts, LoopbackSocket)
///       .initWithSocket(cli_cfg, pair.clientSocket(cli_addr));
///
///   cli_transport.connect();
///   while (not_connected) { cli_transport.tick(); srv_transport.tick(); }
///
const std = @import("std");
const Address = @import("../root.zig").Address;
pub const RecvResult = @import("socket.zig").RecvResult;

/// Maximum packet bytes that fit in a single loopback queue entry.
/// 4096 is far above any realistic zanoza packet for testing purposes.
const MAX_PACKET: usize = 4096;

/// How many packets each direction can buffer before dropping.
const QUEUE_CAP: usize = 64;

// ---------------------------------------------------------------------------
// Internal fixed-size packet queue
// ---------------------------------------------------------------------------

const Entry = struct {
    data: [MAX_PACKET]u8 = undefined,
    len: usize = 0,
    /// The source address stored by the sender (its own local_addr).
    src: Address = undefined,
};

const Queue = struct {
    entries: [QUEUE_CAP]Entry = @splat(.{}),
    head: usize = 0,
    tail: usize = 0,
    count: usize = 0,

    fn push(self: *Queue, src: Address, data: []const u8) void {
        if (self.count >= QUEUE_CAP) return; // silently drop when full
        const len = @min(data.len, MAX_PACKET);
        const e = &self.entries[self.tail];
        e.src = src;
        e.len = len;
        @memcpy(e.data[0..len], data[0..len]);
        self.tail = (self.tail + 1) % QUEUE_CAP;
        self.count += 1;
    }

    fn pop(self: *Queue) ?Entry {
        if (self.count == 0) return null;
        const e = self.entries[self.head];
        self.head = (self.head + 1) % QUEUE_CAP;
        self.count -= 1;
        return e;
    }
};

// ---------------------------------------------------------------------------
// LoopbackSocket
// ---------------------------------------------------------------------------

pub const LoopbackSocket = struct {
    /// Packets this socket sends end up here (the other side's recv).
    send_queue: *Queue,
    /// Packets this socket receives come from here (the other side's send).
    recv_queue: *Queue,
    /// This socket's "own" address, stamped as the source on every sendto.
    local_addr: Address,

    // ------------------------------------------------------------------
    // SocketType interface
    // ------------------------------------------------------------------

    /// Satisfies the interface signature but always returns an error.
    /// Use `Pair.serverSocket` / `Pair.clientSocket` to create connected sockets.
    pub fn open(addr: Address) !LoopbackSocket {
        _ = addr;
        return error.NotSupported;
    }

    pub fn close(self: *LoopbackSocket) void {
        _ = self;
    }

    pub fn recvfrom(self: *LoopbackSocket, buf: []u8) ?RecvResult {
        const e = self.recv_queue.pop() orelse return null;
        const len = @min(e.len, buf.len);
        @memcpy(buf[0..len], e.data[0..len]);
        return .{ .addr = e.src, .len = len };
    }

    /// `addr` is the destination; for loopback we ignore it and stamp
    /// `local_addr` as the source so the other side identifies us correctly.
    pub fn sendto(self: *LoopbackSocket, addr: Address, data: []const u8) void {
        _ = addr;
        self.send_queue.push(self.local_addr, data);
    }

    // ------------------------------------------------------------------
    // Pair: the shared queue storage
    // ------------------------------------------------------------------

    pub const Pair = struct {
        /// Packets sent by the server socket, received by the client socket.
        srv_to_cli: Queue = .{},
        /// Packets sent by the client socket, received by the server socket.
        cli_to_srv: Queue = .{},

        /// Returns a `LoopbackSocket` representing the server side.
        /// `local_addr` will be reported as the source address to the client.
        pub fn serverSocket(self: *Pair, local_addr: Address) LoopbackSocket {
            return .{
                .send_queue = &self.srv_to_cli,
                .recv_queue = &self.cli_to_srv,
                .local_addr = local_addr,
            };
        }

        /// Returns a `LoopbackSocket` representing the client side.
        /// `local_addr` will be reported as the source address to the server.
        pub fn clientSocket(self: *Pair, local_addr: Address) LoopbackSocket {
            return .{
                .send_queue = &self.cli_to_srv,
                .recv_queue = &self.srv_to_cli,
                .local_addr = local_addr,
            };
        }
    };
};
