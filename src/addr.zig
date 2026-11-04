const std = @import("std");
const Address = std.Io.net.IpAddress;

pub const AddressKey = struct {
    pub const WIRE_SIZE: usize = 18;

    ip: [16]u8, // IPv4 mapped into IPv6 or raw IPv6
    port: u16,

    pub fn fromAddress(addr: Address) AddressKey {
        var out: AddressKey = .{
            .ip = @as([16]u8, @splat(0)),
            .port = 0,
        };

        switch (addr) {
            .ip4 => |a| {
                // 0000:0000:0000:0000:0000:ffff:XXXX:XXXX
                out.ip[10] = 0xff;
                out.ip[11] = 0xff;
                @memcpy(out.ip[12..16], a.bytes[0..]);
                out.port = a.port;
            },
            .ip6 => |a| {
                @memcpy(out.ip[0..16], a.bytes[0..]);
                out.port = a.port;
            },
        }

        return out;
    }

    pub fn hash(self: AddressKey) u64 {
        return std.hash.Wyhash.hash(0, std.mem.asBytes(&self));
    }

    pub fn eql(a: AddressKey, b: AddressKey) bool {
        return std.mem.eql(u8, std.mem.asBytes(&a), std.mem.asBytes(&b));
    }

    pub fn encode(self: AddressKey, out: *[WIRE_SIZE]u8) void {
        @memcpy(out[0..16], self.ip[0..]);
        std.mem.writeInt(u16, out[16..18], self.port, .big);
    }

    pub fn decode(bytes: *const [WIRE_SIZE]u8) AddressKey {
        var ip: [16]u8 = undefined;
        @memcpy(ip[0..], bytes[0..16]);
        return .{
            .ip = ip,
            .port = std.mem.readInt(u16, bytes[16..18], .big),
        };
    }
};
