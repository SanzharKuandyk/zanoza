const std = @import("std");
const root = @import("../root.zig");

pub fn validate(comptime T: type, comptime user_data_size: usize) void {
    if (!@hasDecl(T, "wire_size"))
        @compileError("ConnectToken must have: pub const wire_size: usize");

    if (!@hasDecl(T, "encode"))
        @compileError("ConnectToken must have: pub fn encode(*const @This(), *[wire_size]u8) void");
    switch (@typeInfo(@TypeOf(T.encode))) {
        .@"fn" => |fi| {
            if (fi.params.len != 2)
                @compileError("ConnectToken.encode must take exactly 2 parameters");
            const p0 = fi.params[0].type orelse @compileError("ConnectToken.encode param[0] must be *const @This()");
            if (p0 != *const T)
                @compileError("ConnectToken.encode param[0] must be *const @This(), got " ++ @typeName(p0));
            const p1 = fi.params[1].type orelse @compileError("ConnectToken.encode param[1] must be *[wire_size]u8");
            if (p1 != *[T.wire_size]u8)
                @compileError("ConnectToken.encode param[1] must be *[wire_size]u8, got " ++ @typeName(p1));
            const ret = fi.return_type orelse @compileError("ConnectToken.encode must return void");
            if (ret != void)
                @compileError("ConnectToken.encode must return void, got " ++ @typeName(ret));
        },
        else => @compileError("ConnectToken.encode must be a function"),
    }

    if (!@hasDecl(T, "decode"))
        @compileError("ConnectToken must have: pub fn decode(*const [wire_size]u8) ?@This()");
    switch (@typeInfo(@TypeOf(T.decode))) {
        .@"fn" => |fi| {
            if (fi.params.len != 1)
                @compileError("ConnectToken.decode must take exactly 1 parameter");
            const p0 = fi.params[0].type orelse @compileError("ConnectToken.decode param[0] must be *const [wire_size]u8");
            if (p0 != *const [T.wire_size]u8)
                @compileError("ConnectToken.decode param[0] must be *const [wire_size]u8, got " ++ @typeName(p0));
            const ret = fi.return_type orelse @compileError("ConnectToken.decode must return ?@This()");
            switch (@typeInfo(ret)) {
                .optional => |opt| {
                    if (opt.child != T)
                        @compileError("ConnectToken.decode must return ?@This(), got ?" ++ @typeName(opt.child));
                },
                else => @compileError("ConnectToken.decode must return ?@This()"),
            }
        },
        else => @compileError("ConnectToken.decode must be a function"),
    }

    if (!@hasDecl(T, "verify"))
        @compileError("ConnectToken must have: pub fn verify(*const @This(), u64, *const [32]u8) bool");
    switch (@typeInfo(@TypeOf(T.verify))) {
        .@"fn" => |fi| {
            if (fi.params.len != 3)
                @compileError("ConnectToken.verify must take exactly 3 parameters");
            const p0 = fi.params[0].type orelse @compileError("ConnectToken.verify param[0] must be *const @This()");
            if (p0 != *const T)
                @compileError("ConnectToken.verify param[0] must be *const @This(), got " ++ @typeName(p0));
            const p1 = fi.params[1].type orelse @compileError("ConnectToken.verify param[1] must be u64");
            if (p1 != u64)
                @compileError("ConnectToken.verify param[1] must be u64, got " ++ @typeName(p1));
            const p2 = fi.params[2].type orelse @compileError("ConnectToken.verify param[2] must be *const [32]u8");
            if (p2 != *const [root.SECRET_KEY_SIZE]u8)
                @compileError("ConnectToken.verify param[2] must be *const [32]u8, got " ++ @typeName(p2));
            const ret = fi.return_type orelse @compileError("ConnectToken.verify must return bool");
            if (ret != bool)
                @compileError("ConnectToken.verify must return bool, got " ++ @typeName(ret));
        },
        else => @compileError("ConnectToken.verify must be a function"),
    }

    if (!@hasDecl(T, "authorizeAddress"))
        @compileError("ConnectToken must have: pub fn authorizeAddress(*const @This(), zanoza.Address) bool");
    switch (@typeInfo(@TypeOf(T.authorizeAddress))) {
        .@"fn" => |fi| {
            if (fi.params.len != 2)
                @compileError("ConnectToken.authorizeAddress must take exactly 2 parameters");
            const p0 = fi.params[0].type orelse @compileError("ConnectToken.authorizeAddress param[0] must be *const @This()");
            if (p0 != *const T)
                @compileError("ConnectToken.authorizeAddress param[0] must be *const @This(), got " ++ @typeName(p0));
            const p1 = fi.params[1].type orelse @compileError("ConnectToken.authorizeAddress param[1] must be zanoza.Address");
            if (p1 != root.Address)
                @compileError("ConnectToken.authorizeAddress param[1] must be zanoza.Address, got " ++ @typeName(p1));
            const ret = fi.return_type orelse @compileError("ConnectToken.authorizeAddress must return bool");
            if (ret != bool)
                @compileError("ConnectToken.authorizeAddress must return bool, got " ++ @typeName(ret));
        },
        else => @compileError("ConnectToken.authorizeAddress must be a function"),
    }

    if (!@hasField(T, "user_data"))
        @compileError("ConnectToken must have field: user_data: [opts.user_data_size]u8");
    if (@FieldType(T, "user_data") != [user_data_size]u8)
        @compileError("ConnectToken.user_data must be [opts.user_data_size]u8");
}
