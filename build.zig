const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const mod = b.addModule("accord", .{
        .root_source_file = b.path("src/accord.zig"),
        .target = target,
        .optimize = optimize,
    });

    const exe = b.addExecutable(.{
        .name = "accord-lite",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "accord", .module = mod },
            },
        }),
    });
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    run_cmd.addPassthruArgs();
    b.step("run", "Run the local socket demo").dependOn(&run_cmd.step);

    const mod_tests = b.addTest(.{ .root_module = mod });
    const run_mod_tests = b.addRunArtifact(mod_tests);
    const test_step = b.step("test", "Run protocol tests");
    test_step.dependOn(&run_mod_tests.step);

    const eval_accord = b.createModule(.{
        .root_source_file = b.path("src/accord.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const eval_exe = b.addExecutable(.{
        .name = "accord-eval",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/eval.zig"),
            .target = target,
            .optimize = .ReleaseFast,
            .imports = &.{
                .{ .name = "accord", .module = eval_accord },
            },
        }),
    });
    const eval_cmd = b.addRunArtifact(eval_exe);
    eval_cmd.step.dependOn(b.getInstallStep());
    b.step("eval", "Compare Accord framing against JSON/HTTP/gRPC-like codecs").dependOn(&eval_cmd.step);

    const load_accord = b.createModule(.{
        .root_source_file = b.path("src/accord.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const load_exe = b.addExecutable(.{
        .name = "accord-load",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/load.zig"),
            .target = target,
            .optimize = .ReleaseFast,
            .imports = &.{
                .{ .name = "accord", .module = load_accord },
            },
        }),
    });
    const load_cmd = b.addRunArtifact(load_exe);
    b.step("load", "Concurrent connection + 8000-pipeline load model").dependOn(&load_cmd.step);

    const real_exe = b.addExecutable(.{
        .name = "accord-real",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/real.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "accord", .module = mod },
            },
        }),
    });
    const real_cmd = b.addRunArtifact(real_exe);
    b.step("real", "Agent-shaped scenarios on a live Unix link").dependOn(&real_cmd.step);
}
