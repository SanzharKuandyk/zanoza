const std = @import("std");
const zanoza = @import("zanoza");

const opts: zanoza.Options = .{
    .max_clients = 64,
    .user_data_size = 256,
    .max_payload_size = 512,
};

const Srv = zanoza.TransportServer(opts, void);

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    const challenge_key: [32]u8 = @splat(0);
    const cfg = zanoza.ServerConfig.init(
        1,
        5000,
        10000,
        &.{},
        false,
        challenge_key,
        null,
    );

    const bind = try zanoza.Address.parseIp4("0.0.0.0", 9000);
    var srv = try Srv.init(allocator, cfg, bind);
    defer srv.deinit();

    srv.tick();

    while (srv.pollEvent()) |event| {
        switch (event) {
            .ClientConnected => |e| std.debug.print("connected: {}\n", .{e.cid}),
            .ClientDisconnected => |e| std.debug.print("disconnected: {}\n", .{e.cid}),
        }
    }

    while (srv.peekMessage()) |msg| {
        std.debug.print("msg from {} on ch{}: {} bytes\n", .{ msg.cid, msg.channel_id, msg.len });
        srv.consumeMessage();
    }
}
