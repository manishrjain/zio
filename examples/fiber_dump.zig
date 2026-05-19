// SPDX-FileCopyrightText: 2025 Lukáš Lalinský
// SPDX-License-Identifier: MIT

const std = @import("std");
const zio = @import("zio");

fn sleeper(ms: u64) zio.Cancelable!void {
    try zio.sleep(.fromMilliseconds(ms));
}

fn notifyWaiter(n: *zio.Notify) zio.Cancelable!void {
    try n.wait();
}

pub fn main() !void {
    var rt = try zio.Runtime.init(std.heap.smp_allocator, .{});
    defer rt.deinit();

    // A few fibers that will each park in different ways.
    var h1 = try rt.spawnWith(.{ .spawn_loc = @src() }, sleeper, .{1000});
    defer h1.cancel();
    var h2 = try rt.spawnWith(.{ .spawn_loc = @src() }, sleeper, .{2000});
    defer h2.cancel();

    var n: zio.Notify = .init;
    var h3 = try rt.spawnWith(.{ .spawn_loc = @src() }, notifyWaiter, .{&n});
    defer h3.cancel();

    // Let everyone reach their park.
    try zio.sleep(.fromMilliseconds(50));

    const s = rt.stats();
    std.debug.print("\nfiber stats: total={d}  ready={d}  blocked={d}\n\n", .{ s.total, s.ready, s.blocked });

    var buf: [8192]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try rt.dumpFibers(&w);
    std.debug.print("{s}", .{w.buffered()});
}
