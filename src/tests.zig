/// Integration tests for zanoza server/client state machines and channel logic.
/// These tests exercise the state machines directly — no sockets required.
const std = @import("std");
const testing = std.testing;
const zanoza = @import("root.zig");
const packet_mod = @import("packet.zig");
const channel_mod = @import("channel.zig");
const LoopbackSocket = @import("transport/loopback.zig").LoopbackSocket;
const TransportServer = @import("transport/server.zig").TransportServer;
const TransportClient = @import("transport/client.zig").TransportClient;

// Re-export unit tests from sub-modules so `zig build test` picks them up.
comptime {
    _ = channel_mod;
    _ = @import("ring_buffer.zig");
}

const opts: zanoza.Options = .{
    .max_clients = 4,
    .max_pending_clients = 8,
    .nonce_window = 8,
    .outgoing_queue_size = 32,
    .events_queue_size = 32,
    .user_data_size = 16,
    .max_payload_size = 64,
    .channels = &.{
        .{ .kind = .Unreliable },
        .{ .kind = .UnreliableLatest },
        .{ .kind = .ReliableOrdered, .reliable_buffer = 8 },
        .{ .kind = .ReliableUnordered, .reliable_buffer = 8 },
    },
    .reliable_resend_ns = 100 * std.time.ns_per_ms,
};

const Srv = zanoza.Server(opts);
const Cli = zanoza.Client(opts);
const Pkt = packet_mod.Packet(opts);

fn sleepNs(ns: u64) !void {
    var io_threaded: std.Io.Threaded = .init(testing.allocator, .{});
    defer io_threaded.deinit();
    try std.Io.sleep(io_threaded.io(), .fromNanoseconds(@intCast(ns)), .awake);
}

fn testAddr(port: u16) zanoza.Address {
    return .{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
}

fn serverCfg() zanoza.ServerConfig {
    return zanoza.ServerConfig.init(1, 5000, 10000, &.{}, false, @as([32]u8, @splat(0)), null);
}

fn clientCfg(port: u16) zanoza.ClientConfig {
    return .{ .protocol_id = 1, .server_addr = testAddr(port) };
}

/// Relay all outgoing packets from `src` to `dst.handlePacket`.
/// `src_addr` is the address dst will see as the sender.
fn relayClientToServer(cli: *Cli, srv: *Srv, client_addr: zanoza.Address) !void {
    var buf: [packet_mod.maxPacketSize(opts)]u8 = undefined;
    while (cli.pollOutgoing()) |out| {
        const len = try packet_mod.serialize(opts, out.packet, buf[0..]);
        try srv.handlePacket(client_addr, buf[0..len]);
    }
}

fn relayServerToClient(srv: *Srv, cli: *Cli) !void {
    var buf: [packet_mod.maxPacketSize(opts)]u8 = undefined;
    while (srv.pollOutgoing()) |out| {
        const len = try packet_mod.serialize(opts, out.packet, buf[0..]);
        try cli.handlePacket(buf[0..len]);
    }
}

// ---------------------------------------------------------------------------
// Handshake
// ---------------------------------------------------------------------------

test "server-client plain connect handshake" {
    var srv = try Srv.init(testing.allocator, serverCfg());
    defer srv.deinit();

    var cli = try Cli.init(clientCfg(9001));

    const client_addr = testAddr(50001);
    srv.updateNow();
    cli.updateNow();

    // Step 1: client sends ConnectionRequest
    try cli.connect();
    try relayClientToServer(&cli, &srv, client_addr);

    // Step 2: server sends Challenge
    try relayServerToClient(&srv, &cli);
    try testing.expect(cli.pollEvent() == null);

    // Step 3: client sends ConnectionResponse; server accepts and replies
    try relayClientToServer(&cli, &srv, client_addr);
    try relayServerToClient(&srv, &cli);

    // Client should be Connected
    const cli_ev = cli.pollEvent().?;
    try testing.expect(cli_ev == .Connected);
    try testing.expect(cli.pollEvent() == null);

    // Server should have ClientConnected
    const srv_ev = srv.pollEvent().?;
    try testing.expect(srv_ev == .ClientConnected);
    try testing.expectEqual(@as(u64, 0), srv_ev.ClientConnected.cid);
    try testing.expect(srv.pollEvent() == null);

    std.debug.print("\n  PASS: server-client plain connect handshake\n", .{});
}

test "server disconnects client on Disconnect packet" {
    var srv = try Srv.init(testing.allocator, serverCfg());
    defer srv.deinit();
    var cli = try Cli.init(clientCfg(9002));

    const client_addr = testAddr(50002);
    srv.updateNow();
    cli.updateNow();

    try cli.connect();
    try relayClientToServer(&cli, &srv, client_addr);
    try relayServerToClient(&srv, &cli);
    try relayClientToServer(&cli, &srv, client_addr);
    try relayServerToClient(&srv, &cli);
    _ = cli.pollEvent(); // Connected
    _ = srv.pollEvent(); // ClientConnected

    // Client disconnects
    cli.disconnect();
    try relayClientToServer(&cli, &srv, client_addr);

    const srv_ev = srv.pollEvent().?;
    try testing.expect(srv_ev == .ClientDisconnected);

    std.debug.print("\n  PASS: server disconnects client on Disconnect packet\n", .{});
}

// ---------------------------------------------------------------------------
// Server sendPayload → Client
// ---------------------------------------------------------------------------

test "server disconnects idle client on timeout" {
    const timeout_ns = 1 * std.time.ns_per_ms;
    var srv = try Srv.init(
        testing.allocator,
        zanoza.ServerConfig.init(1, 5000, timeout_ns, &.{}, false, @as([32]u8, @splat(0)), null),
    );
    defer srv.deinit();

    var cli = try Cli.init(clientCfg(9006));

    const client_addr = testAddr(50006);
    srv.updateNow();
    cli.updateNow();

    try cli.connect();
    try relayClientToServer(&cli, &srv, client_addr);
    try relayServerToClient(&srv, &cli);
    try relayClientToServer(&cli, &srv, client_addr);
    try relayServerToClient(&srv, &cli);
    _ = cli.pollEvent(); // Connected

    const connected = srv.pollEvent().?;
    try testing.expect(connected == .ClientConnected);
    const cid = connected.ClientConnected.cid;
    const slot: usize = @intCast(cid);

    try sleepNs(timeout_ns + (1 * std.time.ns_per_ms));
    srv.updateNow();

    const disconnected = srv.pollEvent().?;
    try testing.expect(disconnected == .ClientDisconnected);
    try testing.expectEqual(cid, disconnected.ClientDisconnected.cid);
    try testing.expect(srv.clients[slot] == null);
    try testing.expectEqual(@as(usize, 0), srv.addr_to_slot.count());
    try testing.expectError(error.UnknownClient, srv.sendPayload(cid, "late"));

    std.debug.print("\n  PASS: server disconnects idle client on timeout\n", .{});
}

test "server sendPayload reaches client as message" {
    var srv = try Srv.init(testing.allocator, serverCfg());
    defer srv.deinit();
    var cli = try Cli.init(clientCfg(9003));

    const client_addr = testAddr(50003);
    srv.updateNow();
    cli.updateNow();

    try cli.connect();
    try relayClientToServer(&cli, &srv, client_addr);
    try relayServerToClient(&srv, &cli);
    try relayClientToServer(&cli, &srv, client_addr);
    try relayServerToClient(&srv, &cli);
    _ = cli.pollEvent(); // Connected
    const srv_ev = srv.pollEvent().?; // ClientConnected
    const cid = srv_ev.ClientConnected.cid;

    // Server sends a payload
    const body = [_]u8{ 0xAB, 0xBC, 0xCD };
    try srv.sendPayload(cid, &body);
    try relayServerToClient(&srv, &cli);

    const msg = cli.peekMessage().?;
    const data = cli.payloadData(msg.payload);
    try testing.expectEqual(@as(usize, body.len), data.len);
    try testing.expectEqual(@as(u8, 0xAB), data[0]);
    cli.releasePayload(msg.payload);
    cli.consumeMessage();

    std.debug.print("\n  PASS: server sendPayload reaches client as message\n", .{});
}

// ---------------------------------------------------------------------------
// Client sendPayload → Server
// ---------------------------------------------------------------------------

test "client sendPayload reaches server as message" {
    var srv = try Srv.init(testing.allocator, serverCfg());
    defer srv.deinit();
    var cli = try Cli.init(clientCfg(9004));

    const client_addr = testAddr(50004);
    srv.updateNow();
    cli.updateNow();

    try cli.connect();
    try relayClientToServer(&cli, &srv, client_addr);
    try relayServerToClient(&srv, &cli);
    try relayClientToServer(&cli, &srv, client_addr);
    try relayServerToClient(&srv, &cli);
    _ = cli.pollEvent();
    _ = srv.pollEvent();

    const body = [_]u8{ 0xCD, 0xEE };
    try cli.sendPayload(&body);
    try relayClientToServer(&cli, &srv, client_addr);

    const msg = srv.peekMessage().?;
    const data = srv.payloadData(msg.payload);
    try testing.expectEqual(@as(usize, body.len), data.len);
    try testing.expectEqual(@as(u8, 0xCD), data[0]);
    srv.releasePayload(msg.payload);
    srv.consumeMessage();

    std.debug.print("\n  PASS: client sendPayload reaches server as message\n", .{});
}

// ---------------------------------------------------------------------------
// Channel header helpers (used by transport layer)
// ---------------------------------------------------------------------------

test "channel header round-trip through payload body" {
    var body: [opts.max_payload_size]u8 = @splat(0);

    // Encode a channel 1, seq 42, with user data
    channel_mod.encodeHeader(body[0..channel_mod.HEADER_SIZE], 1, 42);
    const user_data = "hello";
    @memcpy(body[channel_mod.HEADER_SIZE .. channel_mod.HEADER_SIZE + user_data.len], user_data);

    const hdr = channel_mod.Header.decode(&body).?;
    try testing.expect(!hdr.is_ack);
    try testing.expectEqual(@as(u8, 1), hdr.channel_id);
    try testing.expectEqual(@as(u16, 42), hdr.seq);
    try testing.expectEqualStrings(user_data, body[channel_mod.HEADER_SIZE .. channel_mod.HEADER_SIZE + user_data.len]);

    std.debug.print("\n  PASS: channel header round-trip through payload body\n", .{});
}

test "channel ACK header round-trip" {
    var body: [opts.max_payload_size]u8 = @splat(0);
    channel_mod.encodeAck(body[0..channel_mod.HEADER_SIZE], 2, 99);

    const hdr = channel_mod.Header.decode(&body).?;
    try testing.expect(hdr.is_ack);
    try testing.expectEqual(@as(u8, 2), hdr.channel_id);
    try testing.expectEqual(@as(u16, 99), hdr.seq);

    std.debug.print("\n  PASS: channel ACK header round-trip\n", .{});
}

// ---------------------------------------------------------------------------
// UnreliableLatest channel filtering via raw state machine
// ---------------------------------------------------------------------------

test "UnreliableLatest drops older sequence via state machine relay" {
    var srv = try Srv.init(testing.allocator, serverCfg());
    defer srv.deinit();
    var cli = try Cli.init(clientCfg(9005));

    const client_addr = testAddr(50005);
    srv.updateNow();
    cli.updateNow();

    // Connect
    try cli.connect();
    try relayClientToServer(&cli, &srv, client_addr);
    try relayServerToClient(&srv, &cli);
    try relayClientToServer(&cli, &srv, client_addr);
    try relayServerToClient(&srv, &cli);
    _ = cli.pollEvent();
    _ = srv.pollEvent();

    // Simulate two payloads where second has lower channel seq (out-of-order)
    var state: channel_mod.UnreliableLatestState = .{};

    var body1: [opts.max_payload_size]u8 = @splat(0);
    channel_mod.encodeHeader(body1[0..channel_mod.HEADER_SIZE], 1, 10); // ch1, seq=10
    body1[channel_mod.HEADER_SIZE] = 0xAA;

    var body2: [opts.max_payload_size]u8 = @splat(0);
    channel_mod.encodeHeader(body2[0..channel_mod.HEADER_SIZE], 1, 5); // ch1, seq=5 (older)
    body2[channel_mod.HEADER_SIZE] = 0xBB;

    // seq=10 accepted
    try testing.expect(state.accept(10));
    // seq=5 rejected (older)
    try testing.expect(!state.accept(5));
    // seq=11 accepted
    try testing.expect(state.accept(11));

    std.debug.print("\n  PASS: UnreliableLatest drops older sequence via state machine relay\n", .{});
}

// ---------------------------------------------------------------------------
// Reliable channel state: push / ack / buffer-full
// ---------------------------------------------------------------------------

test "reliable channel state stores and acks data" {
    const buf_size = 8; // matches reliable_buffer in opts channels above
    const max_ud = opts.max_payload_size - channel_mod.HEADER_SIZE;
    const R = channel_mod.ReliableState(buf_size, max_ud);
    var state: R = .{};

    const msg = "important data";
    const seq = state.push(msg, 0).?;
    try testing.expectEqual(@as(u16, 1), seq);

    // Entry is active at seq-indexed slot (seq=1 → index 1 % buf_size)
    const idx = @as(usize, seq) % buf_size;
    try testing.expect(state.entries[idx].active);
    try testing.expectEqual(msg.len, state.entries[idx].len);
    try testing.expectEqualStrings(msg, state.entries[idx].data[0..msg.len]);

    _ = state.ack(seq);
    try testing.expect(!state.entries[idx].active);

    std.debug.print("\n  PASS: reliable channel state stores and acks data\n", .{});
}

test "default connect token wire round-trip and address authorization" {
    const Token = zanoza.handshake.DefaultConnectToken(opts.user_data_size, opts.max_token_addresses);

    const secret_key: [zanoza.SECRET_KEY_SIZE]u8 = @splat(7);
    const allowed_addr = testAddr(41000);
    const denied_addr = testAddr(41001);
    const user_data: [opts.user_data_size]u8 = @splat(0x5A);

    const token = try Token.create(99, 10_000, &.{allowed_addr}, user_data, &secret_key);
    var buf: [Token.wire_size]u8 = undefined;
    token.encode(&buf);

    const decoded = Token.decode(&buf).?;
    try testing.expect(decoded.verify(1_000, &secret_key));
    try testing.expect(decoded.authorizeAddress(allowed_addr));
    try testing.expect(!decoded.authorizeAddress(denied_addr));
    try testing.expectEqualStrings(token.user_data[0..], decoded.user_data[0..]);
}

// ---------------------------------------------------------------------------
// Transport-layer integration tests using LoopbackSocket
// ---------------------------------------------------------------------------

const TSrv = TransportServer(opts, LoopbackSocket);
const TCli = TransportClient(opts, LoopbackSocket);

/// Addresses used as fake local endpoints for loopback sockets.
const srv_addr: zanoza.Address = .{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 20000 } };
const cli_addr: zanoza.Address = .{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 20001 } };

/// Server config suitable for transport-layer tests that run tick() with real time.
fn transportServerCfg() zanoza.ServerConfig {
    const ns_per_s = std.time.ns_per_s;
    return zanoza.ServerConfig.init(1, 5 * ns_per_s, 30 * ns_per_s, &.{}, false, @as([32]u8, @splat(0)), null);
}

fn loopbackClientCfg() zanoza.ClientConfig {
    const ns_per_s = std.time.ns_per_s;
    return .{
        .protocol_id = 1,
        .server_addr = srv_addr,
        .connect_timeout_ns = 5 * ns_per_s,
        .timeout_ns = 30 * ns_per_s,
    };
}

/// Run up to `max_ticks` alternating tick() pairs until both the server and
/// client have emitted their first event.  Returns {srv_ev, cli_ev}.
fn handshake(srv: *TSrv, cli: *TCli, max_ticks: usize) !struct { TSrv.Event, TCli.Event } {
    var i: usize = 0;
    while (i < max_ticks) : (i += 1) {
        cli.tick();
        srv.tick();
        const se = srv.pollEvent();
        const ce = cli.pollEvent();
        if (se != null and ce != null) return .{ se.?, ce.? };
        // If only one side fired an event keep running; events queue up
        // independently so the other will fire on a subsequent tick.
        if (se != null or ce != null) {
            // drain the remaining side
            var j: usize = 0;
            while (j < max_ticks) : (j += 1) {
                cli.tick();
                srv.tick();
                const se2 = if (se == null) srv.pollEvent() else null;
                const ce2 = if (ce == null) cli.pollEvent() else null;
                if (se2 != null or ce2 != null)
                    return .{ se orelse se2.?, ce orelse ce2.? };
            }
            return error.HandshakeTimeout;
        }
    }
    return error.HandshakeTimeout;
}

test "transport loopback: connect handshake" {
    var pair: LoopbackSocket.Pair = .{};

    var srv = try TSrv.initWithSocket(testing.allocator, transportServerCfg(), pair.serverSocket(srv_addr));
    defer srv.deinit();

    var cli = try TCli.initWithSocket(loopbackClientCfg(), pair.clientSocket(cli_addr));
    defer cli.deinit();

    try cli.connect();

    const evs = try handshake(&srv, &cli, 20);
    try testing.expect(evs[0] == .ClientConnected);
    try testing.expect(evs[1] == .Connected);

    std.debug.print("\n  PASS: transport loopback: connect handshake\n", .{});
}

test "transport loopback: server sendOnChannel reaches client" {
    var pair: LoopbackSocket.Pair = .{};

    var srv = try TSrv.initWithSocket(testing.allocator, transportServerCfg(), pair.serverSocket(srv_addr));
    defer srv.deinit();

    var cli = try TCli.initWithSocket(loopbackClientCfg(), pair.clientSocket(cli_addr));
    defer cli.deinit();

    try cli.connect();
    const evs = try handshake(&srv, &cli, 20);
    const cid = evs[0].ClientConnected.cid;

    // Send on channel 0 (Unreliable)
    try srv.sendOnChannel(cid, 0, "ping");
    srv.tick();
    cli.tick();

    const msg = cli.pollMessage();
    try testing.expect(msg != null);
    try testing.expectEqual(@as(u8, 0), msg.?.channel_id);
    try testing.expectEqual(@as(usize, "ping".len), msg.?.len);
    try testing.expectEqualStrings("ping", msg.?.data[0.."ping".len]);

    std.debug.print("\n  PASS: transport loopback: server sendOnChannel reaches client\n", .{});
}

test "transport loopback: client sendOnChannel reaches server" {
    var pair: LoopbackSocket.Pair = .{};

    var srv = try TSrv.initWithSocket(testing.allocator, transportServerCfg(), pair.serverSocket(srv_addr));
    defer srv.deinit();

    var cli = try TCli.initWithSocket(loopbackClientCfg(), pair.clientSocket(cli_addr));
    defer cli.deinit();

    try cli.connect();
    const evs = try handshake(&srv, &cli, 20);
    const cid = evs[0].ClientConnected.cid;
    _ = cid;

    // Send on channel 2 (ReliableOrdered)
    try cli.sendOnChannel(2, "hello reliable");
    cli.tick();
    srv.tick();

    const msg = srv.pollMessage();
    try testing.expect(msg != null);
    try testing.expectEqual(@as(u8, 2), msg.?.channel_id);
    try testing.expectEqual(@as(usize, "hello reliable".len), msg.?.len);
    try testing.expectEqualStrings("hello reliable", msg.?.data[0.."hello reliable".len]);

    std.debug.print("\n  PASS: transport loopback: client sendOnChannel reaches server\n", .{});
}

test "transport loopback: ReliableUnordered reaches server" {
    var pair: LoopbackSocket.Pair = .{};

    var srv = try TSrv.initWithSocket(testing.allocator, transportServerCfg(), pair.serverSocket(srv_addr));
    defer srv.deinit();

    var cli = try TCli.initWithSocket(loopbackClientCfg(), pair.clientSocket(cli_addr));
    defer cli.deinit();

    try cli.connect();
    _ = try handshake(&srv, &cli, 20);

    try cli.sendOnChannel(3, "hello unordered");
    cli.tick();
    srv.tick();

    const msg = srv.pollMessage();
    try testing.expect(msg != null);
    try testing.expectEqual(@as(u8, 3), msg.?.channel_id);
    try testing.expectEqualStrings("hello unordered", msg.?.data[0.."hello unordered".len]);

    std.debug.print("\n  PASS: transport loopback: ReliableUnordered reaches server\n", .{});
}

test "transport loopback: peekMessage/consumeMessage zero-copy" {
    var pair: LoopbackSocket.Pair = .{};

    var srv = try TSrv.initWithSocket(testing.allocator, transportServerCfg(), pair.serverSocket(srv_addr));
    defer srv.deinit();

    var cli = try TCli.initWithSocket(loopbackClientCfg(), pair.clientSocket(cli_addr));
    defer cli.deinit();

    try cli.connect();
    const evs = try handshake(&srv, &cli, 20);
    const cid = evs[0].ClientConnected.cid;

    try srv.sendOnChannel(cid, 0, "peek-test");
    srv.tick();
    cli.tick();

    // peek does not consume
    const p1 = cli.peekMessage();
    try testing.expect(p1 != null);
    try testing.expectEqualStrings("peek-test", cli.messageData(p1.?));

    const p2 = cli.peekMessage();
    try testing.expect(p2 != null); // still there
    try testing.expectEqual(p1.?, p2.?); // same pointer

    // consume removes it
    cli.consumeMessage();
    try testing.expect(cli.peekMessage() == null);

    std.debug.print("\n  PASS: transport loopback: peekMessage/consumeMessage zero-copy\n", .{});
}

test "transport loopback: client disconnect" {
    var pair: LoopbackSocket.Pair = .{};

    var srv = try TSrv.initWithSocket(testing.allocator, transportServerCfg(), pair.serverSocket(srv_addr));
    defer srv.deinit();

    var cli = try TCli.initWithSocket(loopbackClientCfg(), pair.clientSocket(cli_addr));
    defer cli.deinit();

    try cli.connect();
    _ = try handshake(&srv, &cli, 20);

    cli.disconnect();
    // Flush disconnect packet to server
    var i: usize = 0;
    while (i < 5) : (i += 1) {
        cli.tick();
        srv.tick();
        if (srv.pollEvent()) |ev| {
            try testing.expect(ev == .ClientDisconnected);
            std.debug.print("\n  PASS: transport loopback: client disconnect\n", .{});
            return;
        }
    }
    return error.DisconnectNotReceived;
}

test "transport loopback: client ignores packets from non-server address" {
    var pair: LoopbackSocket.Pair = .{};

    var cli = try TCli.initWithSocket(loopbackClientCfg(), pair.clientSocket(cli_addr));
    defer cli.deinit();

    var attacker = pair.serverSocket(.{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 20002 } });

    try cli.connect();

    var buf: [packet_mod.maxPacketSize(opts)]u8 = undefined;
    const fake_pkt: Pkt = .{ .Challenge = .{
        .sequence = 1,
        .expires_at = 10_000,
        .token = @as([16]u8, @splat(0xAA)),
    } };
    const len = try packet_mod.serialize(opts, fake_pkt, buf[0..]);
    attacker.sendto(srv_addr, buf[0..len]);

    cli.tick();

    try testing.expectEqual(@as(usize, 1), pair.cli_to_srv.count);
    try testing.expect(cli.pollEvent() == null);
}

test "transport loopback: reliable retransmit after lost ack does not redeliver" {
    var pair: LoopbackSocket.Pair = .{};

    var srv = try TSrv.initWithSocket(testing.allocator, transportServerCfg(), pair.serverSocket(srv_addr));
    defer srv.deinit();

    var cli = try TCli.initWithSocket(loopbackClientCfg(), pair.clientSocket(cli_addr));
    defer cli.deinit();

    try cli.connect();
    _ = try handshake(&srv, &cli, 20);

    try cli.sendOnChannel(2, "reliable");
    cli.tick();
    srv.tick();

    const first = srv.pollMessage().?;
    try testing.expectEqualStrings("reliable", first.data[0.."reliable".len]);

    pair.srv_to_cli = .{}; // drop the first ACK

    try sleepNs(opts.reliable_resend_ns + (20 * std.time.ns_per_ms));
    cli.tick();
    srv.tick();

    try testing.expect(srv.pollMessage() == null);

    cli.tick(); // receive the ACK for the retransmission

    try sleepNs(opts.reliable_resend_ns + (20 * std.time.ns_per_ms));
    cli.tick();
    srv.tick();

    try testing.expect(srv.pollMessage() == null);
}

// ---------------------------------------------------------------------------
// Fragmentation
// ---------------------------------------------------------------------------

const frag_opts: zanoza.Options = .{
    .max_clients = 4,
    .max_pending_clients = 8,
    .nonce_window = 8,
    .outgoing_queue_size = 128,
    .events_queue_size = 32,
    .messages_queue_size = 64,
    .user_data_size = 16,
    // max_payload_size = 64, fragment_size = 16:
    //   9 (FRAG_HEADER_SIZE) + 16 = 25 <= 64 ✓
    //   a 40-byte message needs ceil(40/16) = 3 fragments
    .max_payload_size = 64,
    .channels = &.{
        .{ .kind = .ReliableOrdered, .reliable_buffer = 32, .fragment_size = 16 },
    },
    .reliable_resend_ns = 100 * std.time.ns_per_ms,
};

test "transport loopback: fragmented ReliableOrdered channel assembles correctly" {
    const FSrv = TransportServer(frag_opts, LoopbackSocket);
    const FCli = TransportClient(frag_opts, LoopbackSocket);

    const faddr_srv: zanoza.Address = .{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 21000 } };
    const faddr_cli: zanoza.Address = .{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 21001 } };

    var fpair: LoopbackSocket.Pair = .{};

    var fsrv = try FSrv.initWithSocket(
        testing.allocator,
        zanoza.ServerConfig.init(1, 5 * std.time.ns_per_s, 30 * std.time.ns_per_s, &.{}, false, @as([32]u8, @splat(0)), null),
        fpair.serverSocket(faddr_srv),
    );
    defer fsrv.deinit();

    var fcli = try FCli.initWithSocket(
        .{ .protocol_id = 1, .server_addr = faddr_srv, .connect_timeout_ns = 5 * std.time.ns_per_s, .timeout_ns = 30 * std.time.ns_per_s },
        fpair.clientSocket(faddr_cli),
    );
    defer fcli.deinit();

    try fcli.connect();

    // Handshake
    var cid: u64 = 0;
    for (0..20) |_| {
        fcli.tick();
        fsrv.tick();
        if (fsrv.pollEvent()) |ev| {
            cid = ev.ClientConnected.cid;
            break;
        }
    }
    _ = fcli.pollEvent(); // consume Connected

    // Build a 40-byte message: bytes 0,1,2,...,39
    // fragment_size=16 → 3 fragments (16+16+8 bytes)
    var send_data: [40]u8 = undefined;
    for (0..40) |i| send_data[i] = @intCast(i);

    try fsrv.sendOnChannel(cid, 0, &send_data);

    // Tick enough times to deliver all 3 fragments and their ACKs.
    for (0..10) |_| {
        fsrv.tick();
        fcli.tick();
    }

    const msg = fcli.peekMessage();
    try testing.expect(msg != null);
    const data = fcli.messageData(msg.?);
    try testing.expectEqual(@as(usize, 40), data.len);
    for (0..40) |i| {
        try testing.expectEqual(@as(u8, @intCast(i)), data[i]);
    }
    fcli.consumeMessage();
    try testing.expect(fcli.peekMessage() == null);

    std.debug.print("\n  PASS: transport loopback: fragmented ReliableOrdered channel assembles correctly\n", .{});
}
