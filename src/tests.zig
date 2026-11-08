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
    const max_ud = opts.max_payload_size;
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
    // Header storage is additional. A 40-byte message uses three 16-byte fragments.
    .max_payload_size = 64,
    .channels = &.{
        .{ .kind = .ReliableOrdered, .reliable_buffer = 32, .frag_config = .{ .fragment_size = 16, .policy = .always, .frag_assembly_slots = 8 } },
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

    // Always-fragmented empty messages still have one fragment and no user bytes.
    try fcli.sendOnChannel(0, "");
    try testing.expect(fcli.getStateMachine().peekOutgoing().?.packet.Payload.header.fragment != null);
    for (0..4) |_| {
        fcli.tick();
        fsrv.tick();
    }
    const empty = fsrv.peekMessage() orelse return error.MissingEmptyMessage;
    try testing.expectEqual(@as(usize, 0), fsrv.messageData(empty).len);
    fsrv.consumeMessage();

    std.debug.print("\n  PASS: transport loopback: fragmented ReliableOrdered channel assembles correctly\n", .{});
}

test "packet headers carry delivery metadata and leave the full payload for user bytes" {
    var payload: packet_mod.Payload(opts) = .{ .header = .{ .channel_id = 2, .seq = 42 }, .len = opts.max_payload_size, .body = @splat(0xa5) };
    var wire: [packet_mod.maxPacketSize(opts)]u8 = undefined;
    const size = try packet_mod.serialize(opts, .{ .Payload = payload }, &wire);
    try testing.expectEqual(packet_mod.PAYLOAD_HEADER_SIZE + opts.max_payload_size, size);
    const decoded = (try packet_mod.deserialize(opts, wire[0..size])).Payload;
    try testing.expectEqualDeep(payload.header, decoded.header);
    try testing.expectEqualSlices(u8, &payload.body, wire[decoded.header.wireSize()..size]);

    payload.header.is_ack = true;
    payload.len = 0;
    const ack_size = try packet_mod.serialize(opts, .{ .Payload = payload }, &wire);
    const ack = (try packet_mod.deserialize(opts, wire[0..ack_size])).Payload;
    try testing.expect(ack.header.is_ack);
    try testing.expectEqual(@as(u16, 42), ack.header.seq);
    try testing.expectEqual(packet_mod.PAYLOAD_HEADER_SIZE, ack_size);

    wire[2] |= 0x80;
    try testing.expectError(error.InvalidPacket, packet_mod.deserialize(opts, wire[0..ack_size]));
    wire[2] = 0;
    wire[0] = packet_mod.PROTOCOL_VERSION +% 1;
    try testing.expectError(error.UnsupportedVersion, packet_mod.deserialize(opts, wire[0..ack_size]));
}

test "packet decoder rejects incomplete headers and inconsistent fragment sizes" {
    var payload: packet_mod.Payload(frag_opts) = .{
        .header = .{ .channel_id = 0, .seq = 1, .fragment = .{ .msg_id = 3, .frag_count = 3, .frag_index = 0, .total_size = 40 } },
        .len = 16,
        .body = @splat(0xab),
    };
    var wire: [packet_mod.maxPacketSize(frag_opts)]u8 = undefined;
    const size = try packet_mod.serialize(frag_opts, .{ .Payload = payload }, &wire);
    const decoded = (try packet_mod.deserialize(frag_opts, wire[0..size])).Payload;
    try testing.expectEqualDeep(payload.header, decoded.header);
    try testing.expectEqual(packet_mod.FRAGMENTED_PAYLOAD_HEADER_SIZE + 16, size);
    for (0..size) |truncated| {
        try testing.expectError(error.InvalidPacket, packet_mod.deserialize(frag_opts, wire[0..truncated]));
    }
    // A sender cannot declare 40 bytes complete after supplying one byte.
    payload.header.fragment = .{ .msg_id = 3, .frag_count = 1, .frag_index = 0, .total_size = 40 };
    payload.len = 1;
    try testing.expectError(error.InvalidPacket, packet_mod.serialize(frag_opts, .{ .Payload = payload }, &wire));
    wire[packet_mod.PAYLOAD_HEADER_SIZE + 2] = 1;
    try testing.expectError(error.InvalidPacket, packet_mod.deserialize(frag_opts, wire[0..size]));
}

fn connectTransports(srv: anytype, cli: anytype) !u64 {
    try cli.connect();
    var connected = false;
    var cid: ?u64 = null;
    for (0..20) |_| {
        cli.tick();
        srv.tick();
        while (srv.pollEvent()) |event| {
            if (event == .ClientConnected) cid = event.ClientConnected.cid;
        }
        while (cli.pollEvent()) |event| {
            if (event == .Connected) connected = true;
        }
        if (connected and cid != null) return cid.?;
    }
    return error.HandshakeTimeout;
}

test "transport sends full payloads and mixes ordinary messages with fragments in both directions" {
    inline for (.{ zanoza.ChannelKind.Unreliable, zanoza.ChannelKind.ReliableOrdered, zanoza.ChannelKind.ReliableUnordered }) |kind| {
        const options: zanoza.Options = .{
            .max_clients = 4,
            .max_payload_size = 64,
            .channels = &.{.{ .kind = kind, .frag_config = .{ .fragment_size = 16, .policy = .when_needed, .frag_assembly_slots = 8 }, .reliable_buffer = 32 }},
        };
        const S = TransportServer(options, LoopbackSocket);
        const C = TransportClient(options, LoopbackSocket);
        var pair: LoopbackSocket.Pair = .{};
        var srv = try S.initWithSocket(testing.allocator, transportServerCfg(), pair.serverSocket(srv_addr));
        defer srv.deinit();
        var cli = try C.initWithSocket(loopbackClientCfg(), pair.clientSocket(cli_addr));
        defer cli.deinit();
        const cid = try connectTransports(&srv, &cli);
        const data: [65]u8 = @splat(0x7b);
        for ([_]usize{ 0, 16, 64, 65 }) |len| {
            try srv.sendOnChannel(cid, 0, data[0..len]);
            try cli.sendOnChannel(0, data[0..len]);
            const sh = srv.getStateMachine().peekOutgoing().?.packet.Payload.header;
            const ch = cli.getStateMachine().peekOutgoing().?.packet.Payload.header;
            try testing.expectEqual(len > options.max_payload_size, sh.fragment != null);
            try testing.expectEqual(len > options.max_payload_size, ch.fragment != null);
            for (0..4) |_| {
                srv.tick();
                cli.tick();
            }
            const sm = srv.peekMessage() orelse return error.MissingServerMessage;
            const cm = cli.peekMessage() orelse return error.MissingClientMessage;
            try testing.expectEqualSlices(u8, data[0..len], srv.messageData(sm));
            try testing.expectEqualSlices(u8, data[0..len], cli.messageData(cm));
            srv.consumeMessage();
            cli.consumeMessage();
            try testing.expect(srv.peekMessage() == null and cli.peekMessage() == null);
        }
    }
}

test "ordered fragments retain metadata through reordering and retransmission" {
    const S = TransportServer(frag_opts, LoopbackSocket);
    const C = TransportClient(frag_opts, LoopbackSocket);
    var pair: LoopbackSocket.Pair = .{};
    var srv = try S.initWithSocket(testing.allocator, transportServerCfg(), pair.serverSocket(srv_addr));
    defer srv.deinit();
    var cli = try C.initWithSocket(loopbackClientCfg(), pair.clientSocket(cli_addr));
    defer cli.deinit();
    const cid = try connectTransports(&srv, &cli);
    var wire: [packet_mod.maxPacketSize(frag_opts)]u8 = undefined;
    const data: [40]u8 = @splat(0x31);
    try srv.sendOnChannel(cid, 0, &data);
    const first = srv.getStateMachine().pollOutgoing().?;
    const second = srv.getStateMachine().pollOutgoing().?;
    const third = srv.getStateMachine().pollOutgoing().?;
    const third_len = try packet_mod.serialize(frag_opts, third.packet, &wire);
    try cli.getStateMachine().handlePacket(wire[0..third_len]);
    const second_len = try packet_mod.serialize(frag_opts, second.packet, &wire);
    try cli.getStateMachine().handlePacket(wire[0..second_len]);
    cli.tick();
    try testing.expect(cli.peekMessage() == null);
    // ACK the buffered followers while the first fragment remains lost.
    srv.tick();
    try sleepNs(120 * std.time.ns_per_ms);
    srv.tick();
    cli.tick();
    const msg = cli.peekMessage() orelse return error.MissingReassembledMessage;
    try testing.expectEqualSlices(u8, &data, cli.messageData(msg));
    cli.consumeMessage();
    // Replaying the original first fragment must not redeliver the message.
    const first_len = try packet_mod.serialize(frag_opts, first.packet, &wire);
    try cli.getStateMachine().handlePacket(wire[0..first_len]);
    cli.tick();
    try testing.expect(cli.peekMessage() == null);
}

test "outgoing queue rejection leaves reliable sequences and fragmented messages uncommitted" {
    const options: zanoza.Options = .{
        .max_clients = 4,
        .max_payload_size = 64,
        .outgoing_queue_size = 4,
        .channels = &.{
            .{ .kind = .ReliableOrdered, .reliable_buffer = 8 },
            .{ .kind = .ReliableOrdered, .reliable_buffer = 8, .frag_config = .{ .fragment_size = 16, .policy = .always, .frag_assembly_slots = 8 } },
        },
    };
    const S = TransportServer(options, LoopbackSocket);
    const C = TransportClient(options, LoopbackSocket);
    var pair: LoopbackSocket.Pair = .{};
    var srv = try S.initWithSocket(testing.allocator, transportServerCfg(), pair.serverSocket(srv_addr));
    defer srv.deinit();
    var cli = try C.initWithSocket(loopbackClientCfg(), pair.clientSocket(cli_addr));
    defer cli.deinit();
    const cid = try connectTransports(&srv, &cli);
    const data: [40]u8 = @splat(0x44);
    try cli.sendOnChannel(0, "one");
    try cli.sendOnChannel(0, "two");
    try testing.expectError(error.IoError, cli.sendOnChannel(1, &data));
    try testing.expectEqual(@as(usize, 2), cli.getStateMachine().outgoing.len);
    try cli.sendOnChannel(0, "three");
    try cli.sendOnChannel(0, "four");
    try testing.expectError(error.IoError, cli.sendOnChannel(0, "rejected"));
    for (0..3) |_| {
        cli.tick();
        srv.tick();
    }
    while (srv.peekMessage() != null) srv.consumeMessage();
    try cli.sendOnChannel(0, "retry");
    try testing.expectEqual(@as(u16, 5), cli.getStateMachine().peekOutgoing().?.packet.Payload.header.seq);
    for (0..3) |_| {
        cli.tick();
        srv.tick();
    }
    while (srv.peekMessage() != null) srv.consumeMessage();
    try cli.sendOnChannel(1, &data);
    const fragment = cli.getStateMachine().peekOutgoing().?.packet.Payload.header.fragment.?;
    try testing.expectEqual(@as(u16, 1), fragment.msg_id);
    try testing.expectEqual(@as(u16, 1), cli.getStateMachine().peekOutgoing().?.packet.Payload.header.seq);

    const too_big: [65]u8 = @splat(0);
    try testing.expectError(error.MessageTooLarge, srv.sendOnChannel(cid, 0, &too_big));
    try testing.expectError(error.MessageTooLarge, cli.sendOnChannel(0, &too_big));
}

const sized_channel_opts: zanoza.Options = .{
    .max_clients = 4,
    .max_payload_size = 64,
    .outgoing_queue_size = 128,
    .channels = &.{
        .{ .kind = .Unreliable },
        .{ .kind = .UnreliableLatest },
        .{
            .kind = .ReliableOrdered,
            .reliable_buffer = 2,
            .ordered_recv_window = 0,
            .frag_config = .{ .policy = .when_needed, .fragment_size = 8, .frag_assembly_slots = 1 },
        },
        .{
            .kind = .ReliableOrdered,
            .reliable_buffer = 7,
            .ordered_recv_window = 5,
            .frag_config = .{ .policy = .when_needed, .fragment_size = 8, .frag_assembly_slots = 3 },
        },
        .{ .kind = .ReliableUnordered, .reliable_buffer = 3, .unordered_recv_window = 4 },
        .{ .kind = .ReliableUnordered, .reliable_buffer = 9, .unordered_recv_window = 13 },
    },
};

test "channel state uses exact configured buffer, window, and assembly types" {
    const state_mod = @import("channel_state.zig");
    var states = state_mod.initStates(sized_channel_opts.channels, sized_channel_opts.max_payload_size, .embedded);
    defer {
        inline for (sized_channel_opts.channels, 0..) |_, index| states[index].deinit(testing.allocator);
    }

    try testing.expectEqual(@as(usize, 0), @sizeOf(@TypeOf(states[0])));
    try testing.expectEqual(@as(usize, 2), states[2].send.entries.len);
    try testing.expectEqual(@as(usize, 7), states[3].send.entries.len);
    try testing.expectEqual(@as(usize, 3), states[4].send.entries.len);
    try testing.expectEqual(@as(usize, 9), states[5].send.entries.len);
    try testing.expectEqual(@as(usize, 4), states[4].recv.seen.len);
    try testing.expectEqual(@as(usize, 13), states[5].recv.seen.len);
    try testing.expectEqual(@as(usize, 1), states[2].fragments.slots.len);
    try testing.expectEqual(@as(usize, 3), states[3].fragments.slots.len);

    try testing.expectEqual(channel_mod.ReliableOrderedRecvAction.drop, try states[2].recv.receive(testing.allocator, 2, "future"));
    try testing.expect(states[2].recv.futures == null);
    try testing.expectEqual(channel_mod.ReliableOrderedRecvAction.ack_only, try states[3].recv.receive(testing.allocator, 6, "future"));
    try testing.expectEqual(@as(usize, 5), states[3].recv.futures.?.len);
    try testing.expectEqual(channel_mod.ReliableOrderedRecvAction.drop, try states[3].recv.receive(testing.allocator, 7, "too far"));

    try testing.expectEqual(channel_mod.ReliableUnorderedRecvAction.deliver, states[4].recv.classify(10));
    try testing.expectEqual(channel_mod.ReliableUnorderedRecvAction.stale, states[4].recv.classify(6));
    try testing.expectEqual(channel_mod.ReliableUnorderedRecvAction.deliver, states[5].recv.classify(10));
    try testing.expectEqual(channel_mod.ReliableUnorderedRecvAction.deliver, states[5].recv.classify(6));
}

test "each transport enforces its selected channel's reliable capacity" {
    const S = TransportServer(sized_channel_opts, LoopbackSocket);
    const C = TransportClient(sized_channel_opts, LoopbackSocket);
    var pair: LoopbackSocket.Pair = .{};
    var srv = try S.initWithSocket(testing.allocator, transportServerCfg(), pair.serverSocket(srv_addr));
    defer srv.deinit();
    var cli = try C.initWithSocket(loopbackClientCfg(), pair.clientSocket(cli_addr));
    defer cli.deinit();
    const cid = try connectTransports(&srv, &cli);

    inline for (sized_channel_opts.channels, 0..) |config, channel_id| {
        if (config.kind == .ReliableOrdered or config.kind == .ReliableUnordered) {
            try testing.expect(srv.channel_states[cid][channel_id].send == null);
            for (0..config.reliable_buffer) |_| {
                try cli.sendOnChannel(@intCast(channel_id), "data");
                try srv.sendOnChannel(cid, @intCast(channel_id), "data");
            }
            try testing.expectError(error.ReliableBufferFull, cli.sendOnChannel(@intCast(channel_id), "overflow"));
            try testing.expectError(error.ReliableBufferFull, srv.sendOnChannel(cid, @intCast(channel_id), "overflow"));
            try testing.expectEqual(config.reliable_buffer, srv.channel_states[cid][channel_id].send.?.entries.len);
        }
    }
    // The runtime dispatch also reaches channels with no reliable state.
    try cli.sendOnChannel(0, "unreliable");
    try cli.sendOnChannel(1, "latest");
    try srv.sendOnChannel(cid, 0, "unreliable");
    try srv.sendOnChannel(cid, 1, "latest");
    try testing.expectError(error.InvalidChannel, cli.sendOnChannel(255, "invalid"));
    try testing.expectError(error.InvalidChannel, srv.sendOnChannel(cid, 255, "invalid"));

    for (0..4) |_| {
        cli.tick();
        srv.tick();
    }
    var server_counts: [sized_channel_opts.channels.len]usize = @splat(0);
    var client_counts: [sized_channel_opts.channels.len]usize = @splat(0);
    while (srv.peekMessage()) |message| {
        server_counts[message.channel_id] += 1;
        srv.consumeMessage();
    }
    while (cli.peekMessage()) |message| {
        client_counts[message.channel_id] += 1;
        cli.consumeMessage();
    }
    try testing.expectEqualDeep([_]usize{ 1, 1, 2, 7, 3, 9 }, server_counts);
    try testing.expectEqualDeep(server_counts, client_counts);

    inline for (sized_channel_opts.channels, 0..) |config, channel_id| {
        if (config.kind == .ReliableOrdered or config.kind == .ReliableUnordered) {
            try cli.sendOnChannel(@intCast(channel_id), "after ACK");
            try srv.sendOnChannel(cid, @intCast(channel_id), "after ACK");
        }
    }
    cli.disconnect();
}

test "per-channel assembly counts control interleaved messages and reset cleanup" {
    const options: zanoza.Options = .{
        .max_clients = 4,
        .max_payload_size = 64,
        .channels = &.{
            .{ .kind = .Unreliable, .frag_config = .{ .policy = .always, .fragment_size = 8, .frag_assembly_slots = 1 } },
            .{ .kind = .Unreliable, .frag_config = .{ .policy = .always, .fragment_size = 8, .frag_assembly_slots = 3 } },
        },
    };
    const S = TransportServer(options, LoopbackSocket);
    const C = TransportClient(options, LoopbackSocket);
    var pair: LoopbackSocket.Pair = .{};
    var srv = try S.initWithSocket(testing.allocator, transportServerCfg(), pair.serverSocket(srv_addr));
    defer srv.deinit();
    var cli = try C.initWithSocket(loopbackClientCfg(), pair.clientSocket(cli_addr));
    defer cli.deinit();
    const cid = try connectTransports(&srv, &cli);

    const Fragment = struct { msg_id: u16, index: u8 };
    const fragments = [_]Fragment{
        .{ .msg_id = 1, .index = 0 },
        .{ .msg_id = 2, .index = 0 },
        .{ .msg_id = 2, .index = 1 },
        .{ .msg_id = 1, .index = 1 },
    };
    var wire: [packet_mod.maxPacketSize(options)]u8 = undefined;
    for (0..options.channels.len) |channel_id| {
        for (fragments) |fragment| {
            const packet: packet_mod.Packet(options) = .{ .Payload = .{
                .header = .{
                    .channel_id = @intCast(channel_id),
                    .fragment = .{ .msg_id = fragment.msg_id, .frag_count = 2, .frag_index = fragment.index, .total_size = 16 },
                },
                .len = 8,
                .body = @splat(@intCast(fragment.msg_id)),
            } };
            const len = try packet_mod.serialize(options, packet, &wire);
            try srv.getStateMachine().handlePacket(cli_addr, wire[0..len]);
            try cli.getStateMachine().handlePacket(wire[0..len]);
            srv.tick();
            cli.tick();
        }
    }
    var server_counts: [2]usize = @splat(0);
    var client_counts: [2]usize = @splat(0);
    while (srv.peekMessage()) |message| {
        try testing.expectEqual(@as(usize, 16), srv.messageData(message).len);
        server_counts[message.channel_id] += 1;
        srv.consumeMessage();
    }
    while (cli.peekMessage()) |message| {
        try testing.expectEqual(@as(usize, 16), cli.messageData(message).len);
        client_counts[message.channel_id] += 1;
        cli.consumeMessage();
    }
    try testing.expectEqualDeep([_]usize{ 1, 2 }, server_counts);
    try testing.expectEqualDeep(server_counts, client_counts);
    try testing.expect(cli.channel_states[0].fragments.slots[0].active);
    try testing.expect(srv.channel_states[cid][0].fragments.slots[0].active);

    cli.disconnect();
    for (0..4) |_| {
        cli.tick();
        srv.tick();
    }
    try testing.expect(!srv.channel_states[cid][0].fragments.slots[0].active);
    const reconnected_cid = try connectTransports(&srv, &cli);
    try testing.expect(!cli.channel_states[0].fragments.slots[0].active);
    try testing.expect(!srv.channel_states[reconnected_cid][0].fragments.slots[0].active);
    try cli.sendOnChannel(0, "reused");
    for (0..4) |_| {
        cli.tick();
        srv.tick();
    }
    const message = srv.peekMessage() orelse return error.MissingMessageAfterReconnect;
    try testing.expectEqualStrings("reused", srv.messageData(message));
    srv.consumeMessage();
}
