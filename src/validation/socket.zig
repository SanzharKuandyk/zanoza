const std = @import("std");
const Address = std.Io.net.IpAddress;

pub fn validate(comptime T: type) void {
    checkOpen(T);
    checkClose(T);
    checkRecvfrom(T);
    checkSendto(T);
}

fn checkOpen(comptime T: type) void {
    if (!@hasDecl(T, "open"))
        @compileError("Socket missing: pub fn open(zanoza.Address) !" ++ @typeName(T));
    switch (@typeInfo(@TypeOf(T.open))) {
        .@"fn" => |fi| {
            if (fi.param_types.len != 1)
                @compileError("Socket.open must take exactly one parameter (zanoza.Address)");
            const p0 = fi.param_types[0] orelse
                @compileError("Socket.open parameter must be a concrete type (zanoza.Address)");
            if (p0 != Address)
                @compileError("Socket.open parameter must be zanoza.Address, got " ++ @typeName(p0));
            const ret = fi.return_type orelse
                @compileError("Socket.open return type must be !" ++ @typeName(T) ++ ", not anytype");
            switch (@typeInfo(ret)) {
                .error_union => |eu| {
                    if (eu.payload != T)
                        @compileError("Socket.open error-union payload must be " ++ @typeName(T) ++ ", got " ++ @typeName(eu.payload));
                },
                else => @compileError("Socket.open must return an error union (!" ++ @typeName(T) ++ ")"),
            }
        },
        else => @compileError("Socket.open must be a function"),
    }
}

fn checkClose(comptime T: type) void {
    if (!@hasDecl(T, "close"))
        @compileError("Socket missing: pub fn close(*" ++ @typeName(T) ++ ") void");
    switch (@typeInfo(@TypeOf(T.close))) {
        .@"fn" => |fi| {
            if (fi.param_types.len != 1)
                @compileError("Socket.close must take exactly one parameter (*" ++ @typeName(T) ++ ")");
            const p0 = fi.param_types[0] orelse
                @compileError("Socket.close parameter must be a concrete type (*" ++ @typeName(T) ++ ")");
            if (p0 != *T)
                @compileError("Socket.close parameter must be *" ++ @typeName(T) ++ ", got " ++ @typeName(p0));
            const ret = fi.return_type orelse
                @compileError("Socket.close return type must be void, not anytype");
            if (ret != void)
                @compileError("Socket.close must return void, got " ++ @typeName(ret));
        },
        else => @compileError("Socket.close must be a function"),
    }
}

fn checkRecvfrom(comptime T: type) void {
    if (!@hasDecl(T, "recvfrom"))
        @compileError("Socket missing: pub fn recvfrom(*" ++ @typeName(T) ++ ", []u8) ?RecvResult");
    switch (@typeInfo(@TypeOf(T.recvfrom))) {
        .@"fn" => |fi| {
            if (fi.param_types.len != 2)
                @compileError("Socket.recvfrom must take exactly 2 parameters (*T, []u8)");
            const p0 = fi.param_types[0] orelse
                @compileError("Socket.recvfrom first parameter must be *" ++ @typeName(T));
            if (p0 != *T)
                @compileError("Socket.recvfrom first parameter must be *" ++ @typeName(T) ++ ", got " ++ @typeName(p0));
            const p1 = fi.param_types[1] orelse
                @compileError("Socket.recvfrom second parameter must be []u8");
            if (p1 != []u8)
                @compileError("Socket.recvfrom second parameter must be []u8, got " ++ @typeName(p1));
            const ret = fi.return_type orelse
                @compileError("Socket.recvfrom return type must be ?RecvResult, not anytype");
            switch (@typeInfo(ret)) {
                .optional => |opt| {
                    const R = opt.child;
                    if (!@hasField(R, "addr"))
                        @compileError("Socket.recvfrom return type must have field 'addr: zanoza.Address'");
                    if (!@hasField(R, "len"))
                        @compileError("Socket.recvfrom return type must have field 'len: usize'");
                    if (@FieldType(R, "addr") != Address)
                        @compileError("Socket.recvfrom result .addr must be zanoza.Address");
                    if (@FieldType(R, "len") != usize)
                        @compileError("Socket.recvfrom result .len must be usize");
                },
                else => @compileError("Socket.recvfrom must return an optional (?RecvResult)"),
            }
        },
        else => @compileError("Socket.recvfrom must be a function"),
    }
}

fn checkSendto(comptime T: type) void {
    if (!@hasDecl(T, "sendto"))
        @compileError("Socket missing: pub fn sendto(*" ++ @typeName(T) ++ ", zanoza.Address, []const u8) void");
    switch (@typeInfo(@TypeOf(T.sendto))) {
        .@"fn" => |fi| {
            if (fi.param_types.len != 3)
                @compileError("Socket.sendto must take exactly 3 parameters (*T, zanoza.Address, []const u8)");
            const p0 = fi.param_types[0] orelse
                @compileError("Socket.sendto first parameter must be *" ++ @typeName(T));
            if (p0 != *T)
                @compileError("Socket.sendto first parameter must be *" ++ @typeName(T) ++ ", got " ++ @typeName(p0));
            const p1 = fi.param_types[1] orelse
                @compileError("Socket.sendto second parameter must be zanoza.Address");
            if (p1 != Address)
                @compileError("Socket.sendto second parameter must be zanoza.Address, got " ++ @typeName(p1));
            const p2 = fi.param_types[2] orelse
                @compileError("Socket.sendto third parameter must be []const u8");
            if (p2 != []const u8)
                @compileError("Socket.sendto third parameter must be []const u8, got " ++ @typeName(p2));
            const ret = fi.return_type orelse
                @compileError("Socket.sendto return type must be void, not anytype");
            if (ret != void)
                @compileError("Socket.sendto must return void, got " ++ @typeName(ret));
        },
        else => @compileError("Socket.sendto must be a function"),
    }
}
