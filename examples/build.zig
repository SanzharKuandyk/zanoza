const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const zanoza_dep = b.dependency("zanoza", .{
        .target = target,
        .optimize = optimize,
    });
    const zanoza = zanoza_dep.module("zanoza");

    const raylib_dep = b.dependency("raylib_zig", .{
        .target = target,
        .optimize = optimize,
    });
    const raylib = raylib_dep.module("raylib");
    const raylib_artifact = raylib_dep.artifact("raylib");

    const pong_protocol = b.createModule(.{
        .root_source_file = b.path("pong/protocol.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "zanoza", .module = zanoza },
        },
    });

    const pong_server = b.addExecutable(.{
        .name = "pong-server",
        .root_module = b.createModule(.{
            .root_source_file = b.path("pong/server.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zanoza", .module = zanoza },
                .{ .name = "protocol", .module = pong_protocol },
            },
        }),
    });
    b.installArtifact(pong_server);

    const run_pong_server = b.addRunArtifact(pong_server);
    run_pong_server.step.dependOn(b.getInstallStep());
    const pong_server_step = b.step("pong-server", "Run the pong server");
    pong_server_step.dependOn(&run_pong_server.step);

    const pong_client = b.addExecutable(.{
        .name = "pong-client",
        .root_module = b.createModule(.{
            .root_source_file = b.path("pong/client.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zanoza", .module = zanoza },
                .{ .name = "protocol", .module = pong_protocol },
                .{ .name = "raylib", .module = raylib },
            },
        }),
    });
    pong_client.root_module.linkLibrary(raylib_artifact);
    b.installArtifact(pong_client);

    const run_pong_client = b.addRunArtifact(pong_client);
    run_pong_client.step.dependOn(b.getInstallStep());
    // pass args to pong example client
    run_pong_client.addPassthruArgs();
    const pong_client_step = b.step("pong-client", "Run the pong client");
    pong_client_step.dependOn(&run_pong_client.step);

    const ball_protocol = b.createModule(.{
        .root_source_file = b.path("ball/protocol.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "zanoza", .module = zanoza },
        },
    });

    const ball_server = b.addExecutable(.{
        .name = "ball-server",
        .root_module = b.createModule(.{
            .root_source_file = b.path("ball/server.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zanoza", .module = zanoza },
                .{ .name = "protocol", .module = ball_protocol },
            },
        }),
    });
    b.installArtifact(ball_server);

    const run_ball_server = b.addRunArtifact(ball_server);
    run_ball_server.step.dependOn(b.getInstallStep());
    const ball_server_step = b.step("ball-server", "Run the ball server");
    ball_server_step.dependOn(&run_ball_server.step);

    const ball_client = b.addExecutable(.{
        .name = "ball-client",
        .root_module = b.createModule(.{
            .root_source_file = b.path("ball/client.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zanoza", .module = zanoza },
                .{ .name = "protocol", .module = ball_protocol },
                .{ .name = "raylib", .module = raylib },
            },
        }),
    });
    ball_client.root_module.linkLibrary(raylib_artifact);
    b.installArtifact(ball_client);

    const run_ball_client = b.addRunArtifact(ball_client);
    run_ball_client.step.dependOn(b.getInstallStep());
    // pass args to ball example client
    run_ball_client.addPassthruArgs();
    const ball_client_step = b.step("ball-client", "Run the ball client");
    ball_client_step.dependOn(&run_ball_client.step);
}
