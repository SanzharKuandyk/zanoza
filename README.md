# zanoza

Experimental UDP game networking for **Zig 0.17.0**.

Zanoza provides connection handshakes, configurable message channels, fragmentation,
and client/server transports. Channel buffer and assembly capacities are configured
at comptime and stored in per-channel state types.

![Ball example](examples/ball/balls.gif)


| Channel | Delivery |
| --- | --- |
| `Unreliable` | Send once |
| `UnreliableLatest` | Deliver newer sequence numbers and discard older updates |
| `ReliableOrdered` | Acknowledge and retransmit messages, delivering them in order |
| `ReliableUnordered` | Acknowledge and retransmit messages, filtering duplicates |

The API is experimental and may change.

## Try the examples

From the repository root, run the Ball server and client in separate terminals.

```sh
zig build --build-file examples/build.zig ball-server
zig build --build-file examples/build.zig ball-client
```

For Pong, start a server and two clients.

```sh
zig build --build-file examples/build.zig pong-server
zig build --build-file examples/build.zig pong-client
zig build --build-file examples/build.zig pong-client
```

<details>
<summary>Example controls and remote connections</summary>

Ball supports up to four players. Move with `WASD`, open chat with `T`,
and cancel chat with `Esc`.

In Pong, move with `W` / `S`, open chat with `T`, and press `Space` to ready up
after a round. Ball listens on port 9914, and Pong listens on port 9913.

Pass a server IPv4 address after `--` to connect remotely.

```sh
zig build --build-file examples/build.zig ball-client -- 1.2.3.4
zig build --build-file examples/build.zig pong-client -- 1.2.3.4
```

The examples use a separate build package with their GUI dependencies.

</details>

## Quick start

Define a shared channel schema, then select the built-in UDP socket with `void`.

```zig
const zanoza = @import("zanoza");

const options: zanoza.Options = .{
    .max_clients = 4,
    .max_payload_size = 256,
    .channels = &.{
        .{ .kind = .UnreliableLatest },
        .{ .kind = .ReliableOrdered, .reliable_buffer = 16 },
    },
};

const Server = zanoza.TransportServer(options, void);
const Client = zanoza.TransportClient(options, void);
```

Call `tick()` regularly, drain `pollEvent()`, and send after the connection event.
Channel IDs are indexes into `options.channels`, so channel 0 above suits position
updates and channel 1 suits game events.

Read messages with `peekMessage()` and `messageData()`. Call `consumeMessage()`
after processing each message to release its storage.

<details>
<summary>Complete UDP echo example</summary>

This program runs both peers on localhost, sends one reliable message, prints the
reply, and exits. It binds the server to port 9000.

```zig
const std = @import("std");
const zanoza = @import("zanoza");

const options: zanoza.Options = .{
    .max_clients = 4,
    .max_payload_size = 256,
    .channels = &.{
        .{ .kind = .UnreliableLatest },
        .{ .kind = .ReliableOrdered, .reliable_buffer = 16 },
    },
};
const Server = zanoza.TransportServer(options, void);
const Client = zanoza.TransportClient(options, void);

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    var io = std.Io.Threaded.init(allocator, .{});
    defer io.deinit();

    var challenge_key: [32]u8 = undefined;
    io.io().random(&challenge_key);

    const address = try zanoza.Address.parseIp4("127.0.0.1", 9000);
    const server_config: zanoza.ServerConfig = .{
        .protocol_id = 1,
        .handshake_alive_ns = 5 * std.time.ns_per_s,
        .client_timeout_ns = 30 * std.time.ns_per_s,
        .public_addresses = &.{},
        .secure = false,
        .challenge_key = challenge_key,
        .secret_key = null,
    };
    var server = try Server.init(allocator, server_config, address);
    defer server.deinit();

    var client = try Client.init(
        .{ .protocol_id = 1, .server_addr = address },
        try zanoza.Address.parseIp4("127.0.0.1", 0),
    );
    defer client.deinit();
    try client.connect();

    while (true) {
        client.tick();
        server.tick();

        while (client.pollEvent()) |event| {
            if (event == .Connected) try client.sendOnChannel(1, "hello");
        }
        while (server.pollEvent()) |_| {}

        while (server.peekMessage()) |message| {
            try server.sendOnChannel(
                message.cid,
                message.channel_id,
                server.messageData(message),
            );
            server.consumeMessage();
        }
        while (client.peekMessage()) |message| {
            std.debug.print("{s}\n", .{client.messageData(message)});
            client.consumeMessage();
            return;
        }
        try std.Io.sleep(io.io(), .fromNanoseconds(std.time.ns_per_ms), .awake);
    }
}
```

For longer-running applications, handle `Disconnected` / `ClientDisconnected`
events and back off when outgoing or reliable buffers are full. Connection IDs
identify server slots and can be reused after disconnection.

</details>

<details>
<summary>Add the package to your application</summary>

Add the dependency from your application's directory.

```sh
zig fetch --save git+https://github.com/SanzharKuandyk/zanoza
```

After creating your executable in `build.zig`, import the dependency's module.

```zig
const zanoza = b.dependency("zanoza", .{
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("zanoza", zanoza.module("zanoza"));
```

The application can then use `@import("zanoza")`.

</details>

## Reference

<details>
<summary>Configuration and connection lifecycle</summary>

`Options` is passed at comptime to both transport types. Use matching channel IDs,
delivery kinds, fragment sizes, and token layouts on both peers. Receiver payload
limits must accommodate incoming messages. Queue capacities are local limits.

| Option | Default | Meaning |
| --- | --- | --- |
| `max_clients` | `1024` | Server connection slots |
| `max_pending_clients` | `null` | Maximum simultaneous pending handshakes. Defaults to `max_clients * 2`; each pending handshake holds a client slot |
| `max_payload_size` | `1024` | User-data bytes per data packet |
| `outgoing_queue_size` | `256` | Packets waiting to be sent |
| `messages_queue_size` | `256` | Incoming message queue and payload-pool capacity |
| `events_queue_size` | `256` | Connection lifecycle events |
| `nonce_window` | `256` | Recent handshake nonces retained for replay checks |
| `reliable_resend_ns` | `100 * std.time.ns_per_ms` | Minimum retransmission timeout |
| `user_data_size` | `256` | Bytes of application data in a connect token |
| `max_token_addresses` | `8` | Address capacity in the built-in connect token |
| `ConnectToken` | `void` | Selects the built-in token type |
| `channels` | One `Unreliable` channel | Channel schema, up to 128 entries |

Queue sizes and `nonce_window` must be positive powers of two.
`max_pending_clients` must be positive. Payload capacities range from 1 to 65,535 bytes.

Each channel has these settings.

| Field | Default | Meaning |
| --- | --- | --- |
| `kind` | Required | Delivery mode |
| `reliable_buffer` | `64` | Unacknowledged send slots for reliable channels |
| `ordered_recv_window` | `32` | Buffered future packets for `ReliableOrdered`, with `0` disabling buffering |
| `unordered_recv_window` | `64` | Deduplication window for `ReliableUnordered` |
| `frag_config` | `null` | Optional fragmentation settings |

Each channel uses its own configured capacities. Server reliable send buffers and
ordered receive windows are allocated on first use. RTT tracking computes the
retransmission timeout as `max(smoothed_rtt * 2, reliable_resend_ns)`.

`ServerConfig` supplies the protocol ID, handshake lifetime, client timeout,
challenge key, and token-authentication settings. `ClientConfig` supplies the
protocol ID and server address, with these timing defaults.

- `connect_retry_ns` is 500 ms.
- `connect_timeout_ns` is 5 seconds.
- `timeout_ns` is 10 seconds.

The server and client protocol IDs must match. Timeouts measure received traffic,
so active sessions need regular packets. Client events are `Connected` and
`Disconnected`. Server events are `ClientConnected` and `ClientDisconnected`,
carrying the connection ID and peer address. Connected server events also carry
optional token user data.

Configuration definitions are in [root.zig](src/root.zig),
[channel.zig](src/channel.zig), [server/config.zig](src/server/config.zig), and
[client/config.zig](src/client/config.zig).

</details>

<details>
<summary>Fragmentation, packet sizes, and message ownership</summary>

Enable fragmentation through `frag_config` on `Unreliable`, `ReliableOrdered`, or
`ReliableUnordered`. All three configuration fields are required.

```zig
const options: zanoza.Options = .{
    .max_payload_size = 256,
    .channels = &.{.{
        .kind = .ReliableOrdered,
        .reliable_buffer = 64,
        .frag_config = .{
            .policy = .when_needed,
            .fragment_size = 200,
            .frag_assembly_slots = 8,
        },
    }},
};
```

`when_needed` fragments messages exceeding `max_payload_size`. `always` uses
fragment framing for every message, including a single small or empty message.
`UnreliableLatest` supports ordinary messages up to the packet's user-data limit.

`fragment_size` is the maximum user-data bytes per fragment and must be between
1 and `max_payload_size`. `frag_assembly_slots` must be positive and sets the
number of partial messages tracked per channel per peer.

A 600-byte message with 200-byte fragments produces three packets. Each packet
carries its own fragment index, the shared message ID, fragment count, and complete
message size.

| Size | Current limit |
| --- | --- |
| Ordinary data header | 8 bytes, plus user data |
| Fragmented data header | 14 bytes, plus fragment data |
| Complete fragmented message | `min(65_535, 255 * fragment_size)` bytes |
| Fragment count | 255 |
| Send admission | Every fragment must fit the available outgoing slots and, for reliable channels, reliable send slots |

Oversized ordinary messages return `MessageTooLarge`. Reliable fragments are
acknowledged and retransmitted individually. Assembly slot selection uses the
message ID modulo `frag_assembly_slots`, and a collision replaces an incomplete
assembly.

Transport `messageData()` returns the whole assembled message. Its bytes are valid
until `consumeMessage()` or transport cleanup. `pollMessage()` copies at most
`max_payload_size` bytes, so use the peek/read/consume API for large messages.
Consume each message once and copy its data first if the application needs to
retain it.

</details>

<details>
<summary>Error sets and handling capacity limits</summary>

Fallible APIs use named model-wide error sets exported from `zanoza`.

| API | Error set |
| --- | --- |
| Raw client | `ClientError` |
| Raw server | `ServerError` |
| Shared channel/storage failures | `TransportError` |
| Client transport | `TransportClientError = ClientError || TransportError` |
| Server transport | `TransportServerError = ServerError || TransportError` |
| Built-in UDP socket opening | `SocketError` |
| Packet codec | `PacketError` |
| Built-in token creation | `HandshakeError` |

Each client, server, and transport type exposes `Error`. For a transport `T`,
`init` returns `T.InitError!T`, combining `T.Error` with the chosen socket's open
errors. `initWithSocket` uses `T.Error`. Custom sockets define their additional
constructor errors.

`ReliableBufferFull` means the next reliable send slots are awaiting ACKs.
`IoError` means a bounded queue or payload pool is exhausted. Let the transport
tick and retain reliable application messages for a later retry.
`MessageTooLarge` reports a size limit, `InvalidChannel` reports an unknown
channel ID, and `OutOfMemory` reports a failed library allocation.

The transport's `tick()` handles packet-processing and socket I/O outcomes
internally. Its return type is `void`.

Definitions live in [client/error.zig](src/client/error.zig),
[server/error.zig](src/server/error.zig), and
[transport/error.zig](src/transport/error.zig).

</details>

<details>
<summary>Custom sockets and in-memory tests</summary>

Pass a socket type as the second argument to `TransportServer` or
`TransportClient`. The compiler checks these method signatures.

```text
open(addr: zanoza.Address) Error!Self
close(self: *Self) void
recvfrom(self: *Self, buffer: []u8) ?RecvResult
sendto(self: *Self, addr: zanoza.Address, data: []const u8) void

RecvResult has fields addr: zanoza.Address and len: usize.
```

`Error` is the socket's own open error set.

`recvfrom` returns `null` when a datagram is unavailable. `sendto` returns
`void`. A successfully initialized transport owns its supplied socket
and closes it during cleanup.

For in-memory testing, create a `LoopbackSocket.Pair` and use its server/client
endpoints.

```zig
var pair: zanoza.LoopbackSocket.Pair = .{};
var server = try zanoza.TransportServer(options, zanoza.LoopbackSocket)
    .initWithSocket(allocator, server_config, pair.serverSocket(server_address));
defer server.deinit();

var client = try zanoza.TransportClient(options, zanoza.LoopbackSocket)
    .initWithSocket(client_config, pair.clientSocket(client_address));
defer client.deinit();
try client.connect();
```

Keep the pair alive at the same memory location while either transport uses it.
Alternate `client.tick()` and `server.tick()` to drive packets and the handshake.
Each loopback direction buffers up to 64 packets, dropping excess packets.

Run the library tests from the repository root.

```sh
zig build test
zig build test -Doptimize=ReleaseSafe
```

The [integration tests](src/tests.zig) show handshakes, retransmission,
fragmentation, per-channel capacity limits, and custom socket errors.

</details>

<details>
<summary>Raw state machines and packet headers</summary>

`zanoza.Server(options)` and `zanoza.Client(options)` expose the connection state
machines for applications that drive their own I/O. Update their clocks with
`updateNow()` or `update(timestamp)`, feed datagrams to `handlePacket`, and
serialize/drain outgoing packets through your socket.

Raw receive entries contain `header` and a pooled `payload` reference. Release
that reference separately when consuming a raw message.

```zig
while (server.peekMessage()) |message| {
    const bytes = server.payloadData(message.payload);
    std.debug.print("channel={} data={s}\n", .{ message.header.channel_id, bytes });
    server.releasePayload(message.payload);
    server.consumeMessage();
}
```

Raw outgoing slots separate metadata and user bytes.

```zig
const outgoing = try server.reservePayloadSlot(cid);
outgoing.packet.Payload.header = .{ .channel_id = 0, .seq = 1 };
@memcpy(outgoing.packet.Payload.body[0..data.len], data);
outgoing.packet.Payload.len = @intCast(data.len);
```

`sendPayload` is the copying alternative with default header fields. The
transport layer supplies channel ordering, ACK handling, retransmission, and
reassembly around these state machines.

[packet.zig](src/packet.zig) defines the serialized layout and codec.
[channel.zig](src/channel.zig) defines channel configuration and delivery logic.
[channel_state.zig](src/channel_state.zig) builds the per-channel state types.

</details>

<details>
<summary>Connect tokens and current constraints</summary>

The handshake uses HMAC-SHA256 challenge tokens. With `ServerConfig.secure = true`,
connection requests must carry a valid signed connect token. The issuer creates
a token with `zanoza.handshake.DefaultConnectToken(user_data_size, max_addresses).create`,
and the client starts the handshake with `connectSecure(token)`.

Token authentication covers connection requests. Payload bytes travel in plaintext.

Current server call sites pass elapsed nanoseconds since server initialization to
`token.verify(now, secret_key)` and the client's source address to
`token.authorizeAddress(addr)`. Token issuance must account for that clock and
address interpretation. The `ServerConfig.public_addresses` list is stored in
the server configuration.

A custom `Options.ConnectToken` type supplies `wire_size`, `user_data`, `encode`,
`decode`, `verify`, and `authorizeAddress`. The current custom-token validator
uses older reflection fields and needs a Zig 0.17 update before custom token
types can be used on the server.

See [handshake.zig](src/handshake.zig),
[server/server.zig](src/server/server.zig), and
[validation/connect_token.zig](src/validation/connect_token.zig) for the current
contract and validation.

</details>

<details>
<summary>Project notes</summary>

Previously named zenet. Inspired by [renet](https://github.com/lucaspoffo/renet).

AI-assisted project. I wrote part of state-machines and transport myself, but other parts were done with AI with my view on things.
</details>

[MIT license](LICENSE).
