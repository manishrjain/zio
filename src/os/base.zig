const std = @import("std");
const builtin = @import("builtin");
const windows = @import("windows.zig");

pub const iovec = switch (builtin.os.tag) {
    .windows => windows.WSABUF,
    else => std.c.iovec,
};

pub const iovec_const = switch (builtin.os.tag) {
    .windows => windows.WSABUF,
    else => std.c.iovec_const,
};

pub const unexpected_error_tracing = builtin.mode == .Debug and switch (builtin.zig_backend) {
    .stage2_llvm, .stage2_x86_64 => true,
    else => false,
};

pub fn unexpectedError(err: anytype) error{Unexpected} {
    // Always log the unmapped errno — without it, callers see only the
    // opaque `error.Unexpected` and cannot tell which syscall errno fell
    // through the explicit mappings. The Debug-only stack-trace block
    // below is still gated to avoid noise in release builds.
    // `err` here is `anytype` because callers pass either error-set values or
    // raw `linux.E` enum tags. `{}` works for both — `@errorName` only works
    // for the former, so we don't use it.
    std.log.scoped(.zio).err("unexpected errno: {}", .{err});
    // TODO: Setting it to true for now.
    if (true or unexpected_error_tracing) {
        std.debug.print(
            \\unexpected error: {}
            \\please file a bug report: https://github.com/lalinsky/zio/issues/new
            \\
        , .{err});
        if (builtin.zig_version.major == 0 and builtin.zig_version.minor < 16) {
            std.debug.dumpCurrentStackTrace(null);
        } else {
            std.debug.dumpCurrentStackTrace(.{});
        }
    }
    return error.Unexpected;
}
