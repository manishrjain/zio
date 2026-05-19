// SPDX-FileCopyrightText: 2025 Lukáš Lalinský
// SPDX-License-Identifier: MIT

const std = @import("std");
const Allocator = std.mem.Allocator;
const builtin = @import("builtin");
const assert = std.debug.assert;

const ev = @import("ev/root.zig");
const os = @import("os/root.zig");

const meta = @import("meta.zig");
const Cancelable = @import("common.zig").Cancelable;
const log = @import("common.zig").log;
const time = @import("time.zig");
const Duration = time.Duration;
const Timestamp = time.Timestamp;

const Coroutine = @import("coro/coroutines.zig").Coroutine;
const Context = @import("coro/coroutines.zig").Context;
const StackPool = @import("coro/stack_pool.zig").StackPool;
const StackPoolConfig = @import("coro/stack_pool.zig").Config;
const setupStackGrowth = @import("coro/stack.zig").setupStackGrowth;
const cleanupStackGrowth = @import("coro/stack.zig").cleanupStackGrowth;

const AnyTask = @import("task.zig").AnyTask;
const TaskPool = @import("task.zig").TaskPool;
const spawnTask = @import("task.zig").spawnTask;
const finishTask = @import("task.zig").finishTask;
const spawnBlockingTask = @import("blocking_task.zig").spawnBlockingTask;
const Group = @import("group.zig").Group;

const select = @import("select.zig");
const Waiter = @import("common.zig").Waiter;

const mod = @This();

/// Number of executor threads to run (including main).
pub const ExecutorCount = enum(u8) {
    /// Auto-detect based on CPU count
    auto = 0,
    _,

    /// Create an exact executor count (1 = single-threaded, no worker threads)
    pub fn exact(n: u8) ExecutorCount {
        assert(n >= 1 and n <= Executor.max_executors);
        return @enumFromInt(n);
    }

    pub fn resolve(self: ExecutorCount) u8 {
        return switch (self) {
            .auto => @intCast(@min(Executor.max_executors, std.Thread.getCpuCount() catch 1)),
            _ => @intFromEnum(self),
        };
    }
};

// Runtime configuration options
pub const RuntimeOptions = struct {
    thread_pool: ev.ThreadPool.Options = .{},
    stack_pool: StackPoolConfig = .{
        .maximum_size = 8 * 1024 * 1024,
        .committed_size = 256 * 1024,
        .max_unused_stacks = 16,
        .max_age = .fromSeconds(60),
    },
    /// Number of executor threads to run (including main).
    executors: ExecutorCount = .exact(1),
};

const Awaitable = @import("awaitable.zig").Awaitable;

// Public handle for spawned tasks and futures
pub fn JoinHandle(comptime T: type) type {
    return struct {
        const Self = @This();
        pub const Result = T;

        awaitable: ?*Awaitable,
        result: T,

        /// Helper to get result from awaitable and release it
        fn finishAwaitable(self: *Self, awaitable: *Awaitable) void {
            self.result = awaitable.getTypedResult(T);
            awaitable.release();
            self.awaitable = null;
        }

        /// Wait for the task to complete and return its result.
        ///
        /// If the current task is canceled while waiting, the spawned task will be canceled too.
        ///
        /// Example:
        /// ```zig
        /// var handle = try rt.spawn(myTask, .{});
        /// const result = handle.join();
        /// ```
        pub fn join(self: *Self) T {
            // If awaitable is null, result is already cached
            const awaitable = self.awaitable orelse return self.result;

            // Wait for completion
            _ = select.waitUntilComplete(awaitable);

            // Get result and release awaitable
            self.finishAwaitable(awaitable);
            return self.result;
        }

        /// Check if the task has completed and a result is available.
        pub fn hasResult(self: *const Self) bool {
            if (self.awaitable) |awaitable| {
                return awaitable.hasResult();
            }
            return true; // If awaitable is null, result is already cached
        }

        /// Get the result value of type T (preserving any error union).
        /// Asserts that the task has already completed.
        /// This is used internally by select() to preserve error union types.
        pub fn getResult(self: *Self) T {
            if (self.awaitable) |awaitable| {
                return awaitable.getTypedResult(T);
            }
            return self.result;
        }

        /// Registers a waiter to be notified when the task completes.
        /// This is part of the Future protocol for select().
        /// Returns false if the task is already complete (no wait needed), true if added to queue.
        pub fn asyncWait(self: Self, waiter: *Waiter) bool {
            if (self.awaitable) |awaitable| {
                return awaitable.asyncWait(waiter);
            }
            return false; // Already complete
        }

        /// Cancels a pending wait operation by removing the waiter.
        /// This is part of the Future protocol for select().
        /// Returns true if removed, false if already removed by completion (wake in-flight).
        pub fn asyncCancelWait(self: Self, waiter: *Waiter) bool {
            if (self.awaitable) |awaitable| {
                return awaitable.asyncCancelWait(waiter);
            }
            return true; // No awaitable means already completed, no wake in-flight
        }

        /// Request cancellation and wait for the task to complete.
        ///
        /// Safe to call after `join()` - typically used in defer for cleanup.
        ///
        /// Example:
        /// ```zig
        /// var handle = try rt.spawn(myTask, .{});
        /// defer handle.cancel();
        /// // Do some other work that could return early
        /// const result = handle.join();
        /// // cancel() in defer is a no-op since join() already completed
        /// ```
        pub fn cancel(self: *Self) void {
            // If awaitable is null, already completed/detached - no-op
            const awaitable = self.awaitable orelse return;

            // If already done, just clean up
            if (awaitable.hasResult()) {
                self.finishAwaitable(awaitable);
                return;
            }

            // Request cancellation
            awaitable.cancel();

            // Wait for completion
            _ = select.waitUntilComplete(awaitable);

            // Get result and release awaitable
            self.finishAwaitable(awaitable);
        }

        /// Detach the task, allowing it to run in the background.
        ///
        /// After detaching, the result is no longer retrievable.
        ///
        /// Example:
        /// ```zig
        /// var handle = try rt.spawn(backgroundTask, .{});
        /// handle.detach(); // Task runs independently
        /// ```
        pub fn detach(self: *Self) void {
            // If awaitable is null, already detached - no-op
            const awaitable = self.awaitable orelse return;

            awaitable.release();
            self.awaitable = null;
            self.result = undefined;
        }
    };
}

// Generic data structures (private)
const WaitNode = @import("utils/wait_queue.zig").WaitNode;
const ConcurrentStack = @import("utils/concurrent_stack.zig").ConcurrentStack;
const SimpleQueue = @import("utils/simple_queue.zig").SimpleQueue;

comptime {
    // WaitNode needs at least 4-byte alignment for 2 spare bits in pointers
    std.debug.assert(@alignOf(WaitNode) >= 4);
}

/// Scheduling priority for a task. High-priority tasks run before normal ones
/// on the same executor (no starvation prevention — a steady stream of high
/// tasks can defer normal tasks indefinitely).
pub const Priority = enum(u1) {
    normal = 0,
    high = 1,

    pub const count = 2;
};

pub fn getNextExecutor(rt: *Runtime) error{RuntimeShutdown}!*Executor {
    if (rt.shutting_down.load(.acquire)) {
        return error.RuntimeShutdown;
    }

    const index = rt.next_executor_index.fetchAdd(1, .monotonic);
    return rt.executors.items[index % rt.executors.items.len];
}

// Executor - per-thread execution unit for running coroutines
pub const Executor = struct {
    pub const max_executors = 255;

    id: u8,
    loop: ev.Loop,

    // One ready queue per priority level (indexed by @intFromEnum(Priority)).
    // getNextTask serves higher indices (higher priority) first.
    ready_queues: [Priority.count]SimpleQueue(WaitNode) = .{ .{}, .{} },

    // Tracks tasks run since last event loop tick.
    // After EVENT_INTERVAL tasks, getNextTask() returns null to force I/O processing.
    tick_task_count: u8 = 0,

    // Monotonically increasing tick counter, incremented after each event loop tick.
    // Used with task.last_run_tick to prevent running the same task more than once per tick.
    // Starts at 1 so new tasks (last_run_tick=0) can run immediately.
    current_tick: u32 = 1,

    // Tracks tasks waiting in any of ready_queues + next_ready_queue_remote.
    ready_count: u32 = 0,

    // Timestamp of last event loop tick, used for time-based yield decisions.
    last_tick_time: Timestamp = .zero,

    // Deferred cleanup for the task that just yielded away from this executor.
    // Processed by the next coroutine to run (at landing sites: startFn, yield resume, run loop).
    pending_cleanup: TaskCleanup = .none,

    // Remote task support - lock-free LIFO stack for cross-thread resumption
    next_ready_queue_remote: ConcurrentStack(WaitNode) = .{},

    // Back-reference to runtime for global coordination
    runtime: *Runtime,

    // Main task for non-coroutine contexts (e.g., main thread calling rt.sleep())
    // This allows the main thread to use the same yield/wake mechanisms as spawned tasks.
    // Note: main_task.coro is not a real coroutine - scheduleTask handles it specially
    // by setting state to .ready without queuing.
    main_task: AnyTask = undefined,

    // Shutdown event - keeps the event loop active and provides cross-thread shutdown.
    // When notified, it calls loop.stop() to exit the event loop.
    shutdown: ev.Async = ev.Async.init(),

    // Executor dedicated to this thread. Written once on init, never updated.
    pub threadlocal var current: ?*Executor = null;

    /// Get the Executor instance from any coroutine that belongs to it.
    /// Coroutines have parent_context_ptr pointing to main_task.coro.context,
    /// so we navigate: context -> coro -> main_task -> executor.
    /// Only valid on the executor thread that is currently running the coroutine.
    pub fn fromCoroutine(coro: *Coroutine) *Executor {
        const main_coro: *Coroutine = @fieldParentPtr("context", coro.parent_context_ptr);
        const main_task: *AnyTask = @fieldParentPtr("coro", main_coro);
        return @alignCast(@fieldParentPtr("main_task", main_task));
    }

    pub fn init(self: *Executor, runtime: *Runtime, id: u8) !void {
        self.* = .{
            .id = id,
            .loop = undefined,
            .runtime = runtime,
            .shutdown = ev.Async.init(),
        };

        // Initialize main_task - this serves as both the scheduler context and
        // the task context for async operations called from main.
        // main_task.coro.context is where spawned tasks yield back to.
        self.main_task = .{
            .state = std.atomic.Value(AnyTask.State).init(.{ .tag = .ready }),
            .awaitable = .{
                .kind = .task,
                .wait_node = .{},
            },
            .coro = .{
                .context = std.mem.zeroes(Context),
                .parent_context_ptr = undefined,
            },
            .runtime = runtime,
            .closure = undefined, // main_task has no closure
        };
        self.main_task.coro.parent_context_ptr = &self.main_task.coro.context;

        try setupStackGrowth();
        errdefer cleanupStackGrowth();

        try self.loop.init(.{
            .allocator = self.runtime.allocator,
            .thread_pool = &self.runtime.thread_pool,
            .defer_callbacks = false,
        });
        errdefer self.loop.deinit();

        // Register shutdown handle to keep loop active and enable cross-thread shutdown
        self.shutdown.c.callback = shutdownCallback;
        self.loop.add(&self.shutdown.c);

        self.main_task.coro.setCurrent();
        Executor.current = self;
    }

    pub fn deinit(self: *Executor) void {
        Executor.current = null;
        Coroutine.clearCurrent();

        self.loop.deinit();

        cleanupStackGrowth();
    }

    fn shutdownCallback(loop: *ev.Loop, _: *ev.Completion) void {
        loop.stop();
    }

    pub const YieldCancelMode = enum { allow_cancel, no_cancel };

    /// Yield to other tasks only if many are waiting, to balance fairness vs. context-switch overhead.
    const yield_ready_threshold = 13;

    pub fn maybeYield(self: *Executor, comptime mode: AnyTask.YieldMode, comptime cancel_mode: YieldCancelMode) if (cancel_mode == .allow_cancel) Cancelable!void else void {
        if (self.ready_count >= yield_ready_threshold) {
            return getCurrentTask().yield(mode, cancel_mode);
        }
    }

    pub const RunMode = enum {
        /// Run until main_task.state becomes .ready.
        /// Caller must set up the state before calling (e.g., .waiting for I/O).
        until_ready,
        /// Run until explicitly stopped via loop.stop().
        /// Used for worker executor threads.
        until_stopped,
    };

    /// Run the executor event loop.
    pub fn run(self: *Executor, mode: RunMode) !void {
        const check_ready = mode != .until_stopped;

        // Process deferred cleanup (e.g. main task's park/reschedule)
        self.processCleanup();

        while (true) {
            // Process ready coroutines
            while (self.getNextTask()) |next_task| {
                // Store parent_context_ptr just before stepping into the coroutine.
                // Both the store and the subsequent load in fromCoroutine() happen on
                // the same executor thread, so no cross-thread ordering is needed.
                next_task.coro.parent_context_ptr = &self.main_task.coro.context;
                next_task.coro.step();
                self.processCleanup();
            }

            // Exit if loop is stopped
            if (self.loop.stopped()) {
                if (mode == .until_stopped) {
                    return;
                }
                @panic("event loop stopped while the main task was yielding");
            }

            // Drain remote ready queue (cross-thread tasks) after processing current queue.
            // Dispatch each task to the local queue matching its priority.
            var drained = self.next_ready_queue_remote.popAll();
            while (drained.pop()) |node| {
                const task = AnyTask.fromWaitNode(node);
                self.ready_queues[@intFromEnum(task.priority)].push(node);
                self.ready_count += 1;
            }

            // Run event loop - non-blocking if there's work, otherwise wait for I/O
            const main_ready = check_ready and self.main_task.state.load(.acquire).tag == .ready;
            const has_work = self.hasReadyTask() or main_ready;
            try self.loop.run(if (has_work) .no_wait else .once);

            // Reset task counter and update tick time after event loop tick
            self.tick_task_count = 0;
            self.current_tick +%= 1;
            self.last_tick_time = self.loop.now();

            // Check again after I/O
            if (check_ready and self.main_task.state.load(.acquire).tag == .ready) {
                return;
            }
        }
    }

    /// Get the next task to run from the ready queues.
    ///
    /// Higher-priority queues are drained first. Within a priority level, FIFO.
    /// Returns null if no task is available, or if EVENT_INTERVAL tasks have
    /// been run since the last event loop tick (to ensure I/O responsiveness),
    /// or if every non-empty queue's head already ran this tick.
    fn getNextTask(self: *Executor) ?*AnyTask {
        // Maximum tasks to run before forcing an event loop tick (from Go's scheduler)
        const EVENT_INTERVAL = 61;

        // Force event loop tick after running EVENT_INTERVAL tasks
        if (self.tick_task_count >= EVENT_INTERVAL) {
            return null;
        }

        // Serve higher priorities first. Walk from highest enum index down.
        var i: usize = Priority.count;
        while (i > 0) {
            i -= 1;
            if (self.popReady(&self.ready_queues[i])) |task| return task;
        }
        return null;
    }

    /// Try to pop the head of one ready queue. Returns null if the queue is empty,
    /// or if its head already ran this tick (to avoid running the same task twice
    /// per tick — caller should advance the tick and retry).
    fn popReady(self: *Executor, queue: *SimpleQueue(WaitNode)) ?*AnyTask {
        const node = queue.head orelse return null;
        const task = AnyTask.fromWaitNode(node);

        // Task already ran this tick? Leave it in place — the outer run loop
        // will advance the tick and we'll pick it up on the next pass.
        if (task.last_run_tick == self.current_tick) {
            return null;
        }

        _ = queue.pop();
        task.last_run_tick = self.current_tick;
        self.tick_task_count += 1;
        self.ready_count -= 1;
        return task;
    }

    /// True if any priority queue has at least one queued task.
    fn hasReadyTask(self: *const Executor) bool {
        for (&self.ready_queues) |*q| {
            if (q.head != null) return true;
        }
        return false;
    }

    /// Schedule a task to the current executor's local queue.
    /// This must only be called when we're on the correct executor thread.
    fn scheduleTaskLocal(self: *Executor, task: *AnyTask) void {
        // Main task is never queued — the run loop checks its state directly
        if (task == &self.main_task) return;

        const wait_node = &task.awaitable.wait_node;
        if (std.debug.runtime_safety) {
            std.debug.assert(!wait_node.in_list);
        }
        self.ready_queues[@intFromEnum(task.priority)].push(wait_node);
        self.ready_count += 1;
    }

    /// Schedule a task to a remote executor (different executor or no current executor).
    /// Uses the thread-safe remote queue and notifies the executor.
    fn scheduleTaskRemote(self: *Executor, task: *AnyTask) void {
        const wait_node = &task.awaitable.wait_node;
        if (std.debug.runtime_safety) {
            std.debug.assert(!wait_node.in_list);
        }

        // Push to remote ready queue (thread-safe)
        self.next_ready_queue_remote.push(wait_node);

        // Wake the target executor's event loop (only if initialized)
        self.loop.wake();
    }

    /// Schedule a task for execution.
    /// Atomically transitions task state to .ready and schedules it for execution.
    /// May migrate the task to the current executor for cache locality.
    pub fn scheduleTask(task: *AnyTask) void {
        var old = task.state.load(.acquire);
        while (true) {
            switch (old.tag) {
                // Task already finished (race between completion and cancel) - nothing to do
                .finished => return,
                // Task is in .ready state (running or about to park).
                // Set the awaken bit as a park token; processCleanup.park will consume it
                // and reschedule the task instead of transitioning to .waiting.
                .ready => {
                    if (old.awaken) return; // Token already set, nothing to do
                    const desired = AnyTask.State{ .tag = .ready, .awaken = true };
                    if (task.state.cmpxchgWeak(old, desired, .acq_rel, .acquire)) |actual| {
                        old = actual;
                        continue;
                    }
                    return; // Awaken token set; task will handle it before/during next park
                },
                // Valid states to transition to .ready
                .new, .waiting => {},
            }
            const desired = AnyTask.State{ .tag = .ready, .awaken = false };
            if (task.state.cmpxchgWeak(old, desired, .acq_rel, .acquire)) |actual| {
                old = actual;
                continue;
            }
            break;
        }

        // Main task: parent_context_ptr points to its own context (self-referencing, immutable).
        // main_task is never queued - it just checks state in run().
        if (task.coro.parent_context_ptr == &task.coro.context) {
            const home_executor: *Executor = @alignCast(@fieldParentPtr("main_task", task));
            if (getCurrentExecutorOrNull() != home_executor) {
                home_executor.loop.wake();
            }
            return;
        }

        // Normal scheduling. .new tasks always go to their home executor
        // (chosen by spawnTask, either via round-robin getNextExecutor or an
        // explicit pin from spawnWith). Re-routing here would override the
        // caller's placement.
        if (getCurrentExecutorOrNull()) |current_exec| {
            if (current_exec.runtime == task.runtime and old.tag != .new) {
                const home_exec = Executor.fromCoroutine(&task.coro);
                if (current_exec == home_exec or task.canMigrate()) {
                    task.last_run_tick = 0; // Allow immediate execution on new executor
                    current_exec.scheduleTaskLocal(task);
                } else {
                    home_exec.scheduleTaskRemote(task);
                }
                return;
            }
        }

        // No current executor, different runtime, or .new task — go home.
        Executor.fromCoroutine(&task.coro).scheduleTaskRemote(task);
    }

    const TaskCleanup = union(enum) {
        none,
        reschedule: *AnyTask,
        park: *AnyTask,
        finish: *AnyTask,
    };

    /// Process deferred cleanup for the task that just yielded away.
    /// Called at each landing site after a context switch:
    /// - startFn (new task entry)
    /// - yield resume (after yieldTo returns)
    /// - run loop (after step returns)
    pub fn processCleanup(self: *Executor) void {
        switch (self.pending_cleanup) {
            .none => {},
            .reschedule => |task| {
                self.pending_cleanup = .none;
                self.scheduleTaskLocal(task);
            },
            .park => |task| {
                self.pending_cleanup = .none;
                // Context is now saved — safe to make the task wakeable.
                // Atomically check the awaken bit and either:
                // - Transition (ready, awaken=false) → (waiting, awaken=false): normal park
                // - Consume (ready, awaken=true) → (ready, awaken=false): pre-woken, reschedule
                var old = task.state.load(.acquire);
                while (true) {
                    std.debug.assert(old.tag == .ready);
                    if (old.awaken) {
                        // Pre-woken: consume the token, keep .ready, and reschedule
                        const desired = AnyTask.State{ .tag = .ready, .awaken = false };
                        if (task.state.cmpxchgWeak(old, desired, .acq_rel, .acquire)) |actual| {
                            old = actual;
                            continue;
                        }
                        self.scheduleTaskLocal(task);
                        return;
                    }
                    // Normal: transition to .waiting
                    const desired = AnyTask.State{ .tag = .waiting, .awaken = false };
                    if (task.state.cmpxchgWeak(old, desired, .acq_rel, .acquire)) |actual| {
                        old = actual;
                        continue;
                    }
                    break; // Task is now .waiting
                }
            },
            .finish => |task| {
                self.pending_cleanup = .none;
                task.state.store(.{ .tag = .finished }, .release);
                if (task.coro.context.stack_info.allocation_len > 0) {
                    self.runtime.stack_pool.release(task.coro.context.stack_info, self.loop.now());
                    task.coro.context.stack_info.allocation_len = 0;
                }
                finishTask(self.runtime, &task.awaitable);
            },
        }
    }

    /// Yield the current coroutine to the next ready task or back to the run loop.
    /// Sets current_task for the target and performs the context switch.
    pub fn switchOut(self: *Executor, coro: *Coroutine) void {
        if (self.getNextTask()) |next_task| {
            // Store parent_context_ptr just before switching into the coroutine.
            // Both the store and the subsequent load in fromCoroutine() happen on
            // the same executor thread, so no cross-thread ordering is needed.
            next_task.coro.parent_context_ptr = &self.main_task.coro.context;
            coro.yieldTo(&next_task.coro);
        } else {
            coro.yield();
        }
    }
};

/// Get the current thread's executor.
/// Panics if called from a thread without an active executor context.
pub fn getCurrentExecutor() *Executor {
    return getCurrentExecutorOrNull() orelse @panic("no current executor");
}

pub fn getCurrentExecutorOrNull() ?*Executor {
    const task = getCurrentTaskOrNull() orelse return null;
    return task.getExecutor();
}

/// Get the currently executing task.
/// Panics if called from a thread without an active executor context.
pub fn getCurrentTask() *AnyTask {
    return getCurrentTaskOrNull() orelse @panic("no current task");
}

/// Get the currently executing task, or null if not in task context.
pub fn getCurrentTaskOrNull() ?*AnyTask {
    const coro = Coroutine.getCurrent() orelse return null;
    return AnyTask.fromCoroutine(coro);
}

/// Cooperatively yield control to allow other tasks to run.
/// The current task will be rescheduled and continue execution later.
/// Returns error.Canceled if the task was canceled.
/// No-op if called from a thread without an executor (returns without error).
pub fn yield() Cancelable!void {
    const task = getCurrentTaskOrNull() orelse {
        os.thread.yield();
        return;
    };
    return task.yield(.reschedule, .allow_cancel);
}

/// Spawn a task on the current runtime.
/// Panics if called outside of a task context.
pub fn spawn(func: anytype, args: std.meta.ArgsTuple(@TypeOf(func))) !JoinHandle(meta.ReturnType(func)) {
    const rt = getCurrentExecutor().runtime;
    return rt.spawn(func, args);
}

/// Spawn a blocking task on the current runtime.
/// Panics if called outside of a task context.
pub fn spawnBlocking(func: anytype, args: std.meta.ArgsTuple(@TypeOf(func))) !JoinHandle(meta.ReturnType(func)) {
    const rt = getCurrentExecutor().runtime;
    return rt.spawnBlocking(func, args);
}

/// Begin a cancellation shield to prevent being canceled during critical sections.
/// If not in a task context, this is a no-op.
pub fn beginShield() void {
    if (getCurrentTaskOrNull()) |task| {
        task.beginShield();
    }
}

/// End a cancellation shield.
/// If not in a task context, this is a no-op.
pub fn endShield() void {
    if (getCurrentTaskOrNull()) |task| {
        task.endShield();
    }
}

/// Check if the current task has been cancelled and return an error if so.
/// If not in a task context, this is a no-op.
pub fn checkCancel() Cancelable!void {
    if (getCurrentTaskOrNull()) |task| {
        try task.checkCancel();
    }
}

/// Get the current monotonic timestamp.
pub fn now() Timestamp {
    return .now(.monotonic);
}

/// Sleep for a specified duration.
pub fn sleep(duration: Duration) Cancelable!void {
    var waiter: Waiter = .init();
    try waiter.timedWait(1, .{ .duration = duration }, .allow_cancel);
}

// Runtime - orchestrator for one or more Executors
pub const Runtime = struct {
    thread_pool: ev.ThreadPool,
    stack_pool: StackPool,
    task_pool: TaskPool,
    allocator: Allocator,
    options: RuntimeOptions,

    executors: std.ArrayList(*Executor) = .empty,
    main_executor: Executor,
    next_executor_index: std.atomic.Value(usize) = .init(0),
    workers: std.ArrayList(Worker) = .empty,
    task_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0), // Active task counter
    shutting_down: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    // List of live AnyTask fibers (excludes main_task and blocking tasks).
    // Used by stats() / dumpFibers() for diagnostics. Maintained via
    // swap-remove on finish; each task stores its slot index in
    // task.fibers_index. Manipulated by registerTask / finishTask under
    // fibers_mutex.
    fibers: std.ArrayList(*AnyTask) = .empty,
    fibers_mutex: os.Mutex = .init(),

    const Worker = struct {
        thread: std.Thread = undefined,
        ready: os.ResetEvent = .init(),
        err: ?anyerror = null,
        executor: Executor = undefined,
    };

    pub fn init(allocator: Allocator, options: RuntimeOptions) !*Runtime {
        const self = try allocator.create(Runtime);
        errdefer allocator.destroy(self);

        const num_executors = options.executors.resolve();

        self.* = .{
            .allocator = allocator,
            .options = options,
            .thread_pool = undefined,
            .main_executor = undefined,
            .stack_pool = .init(options.stack_pool),
            .task_pool = .init(allocator),
        };

        try self.thread_pool.init(allocator, options.thread_pool);
        errdefer self.thread_pool.deinit();

        try self.executors.ensureTotalCapacity(allocator, num_executors);
        errdefer self.executors.deinit(allocator);

        try self.main_executor.init(self, 0);
        errdefer self.main_executor.deinit();
        self.executors.appendAssumeCapacity(&self.main_executor);

        const num_workers = num_executors - 1;
        try self.workers.ensureTotalCapacity(allocator, num_workers);

        errdefer self.shutdownWorkers();

        for (0..num_workers) |i| {
            log.debug("Spawning worker thread {}", .{i + 1});
            const worker = self.workers.addOneAssumeCapacity();
            errdefer _ = self.workers.pop();
            worker.* = .{};
            worker.thread = try std.Thread.spawn(.{}, runWorker, .{ self, worker, @as(u6, @intCast(i + 1)) });
        }

        for (self.workers.items, 0..) |*worker, i| {
            log.debug("Waiting for worker thread {}", .{i + 1});
            worker.ready.wait();
            if (worker.err) |e| {
                return e;
            }
            self.executors.appendAssumeCapacity(&worker.executor);
        }

        return self;
    }

    /// Stop worker executors and join threads. Used by deinit() and init() error path.
    fn shutdownWorkers(self: *Runtime) void {
        // Wait for all workers to finish initialization, then stop their event loops.
        // Workers that failed to initialize (err != null) don't have valid executors.
        for (self.workers.items) |*worker| {
            worker.ready.wait();
            if (worker.err == null) {
                worker.executor.shutdown.notify();
            }
        }

        // Join worker threads
        for (self.workers.items) |*worker| {
            worker.thread.join();
        }
        self.workers.deinit(self.allocator);
    }

    pub fn deinit(self: *Runtime) void {
        const allocator = self.allocator;

        // Set shutting_down flag to prevent new spawns
        self.shutting_down.store(true, .release);

        // Stop worker executors and join threads
        self.shutdownWorkers();

        // Shutdown thread pool
        self.thread_pool.stop();

        // All tasks should be complete before deinit
        std.debug.assert(self.task_count.load(.acquire) == 0);

        // Worker executors clean themselves up via defer in runWorker.
        // We only need to deinit the main executor here.
        self.main_executor.deinit();

        self.executors.deinit(allocator);

        // Fiber list must be empty by now (task_count == 0 asserted above).
        std.debug.assert(self.fibers.items.len == 0);
        self.fibers.deinit(allocator);

        // Clean up ThreadPool after executors
        self.thread_pool.deinit();

        // Clean up stack pool
        self.stack_pool.deinit();

        // Clean up task pool
        self.task_pool.deinit();

        // Free the Runtime allocation
        allocator.destroy(self);
    }

    // High-level public API
    pub fn spawn(self: *Runtime, func: anytype, args: std.meta.ArgsTuple(@TypeOf(func))) !JoinHandle(meta.ReturnType(func)) {
        const Result = meta.ReturnType(func);
        const Args = @TypeOf(args);

        const Wrapper = struct {
            fn start(ctx: *const anyopaque, result: *anyopaque) void {
                const a: *const Args = @ptrCast(@alignCast(ctx));
                const r: *Result = @ptrCast(@alignCast(result));
                r.* = @call(.auto, func, a.*);
            }
        };

        const task = try spawnTask(
            self,
            @sizeOf(Result),
            .fromByteUnits(@alignOf(Result)),
            std.mem.asBytes(&args),
            .fromByteUnits(@alignOf(Args)),
            .{ .regular = &Wrapper.start },
            null,
            .{},
        );

        return JoinHandle(Result){
            .awaitable = &task.awaitable,
            .result = undefined,
        };
    }

    /// Number of executors this runtime owns. Indices [0, executorCount()) are
    /// valid for SpawnOptions.executor.
    pub fn executorCount(self: *const Runtime) usize {
        return self.executors.items.len;
    }

    /// Snapshot of fiber counters at a point in time. `total` and `ready` are
    /// observed slightly racy across executors but converge under steady state.
    /// `blocked` is computed as `total - ready` (so it includes pure-running
    /// fibers that are momentarily off-queue; in practice that population is
    /// at most one per executor).
    pub const Stats = struct {
        total: u32,
        ready: u32,
        blocked: u32,
    };

    /// Return a snapshot of fiber counters.
    /// Excludes blocking tasks (those scheduled via spawnBlocking).
    pub fn stats(self: *Runtime) Stats {
        // total = registered AnyTask count (rt.task_count also includes
        // blocking tasks, so we read the fiber list length under the mutex).
        self.fibers_mutex.lock();
        const total: u32 = @intCast(self.fibers.items.len);
        self.fibers_mutex.unlock();

        // ready = sum of per-executor ready_count. ready_count is mutated by
        // its owning executor thread without atomics; use a relaxed atomic
        // load for portability. Tearing is benign for a diagnostic snapshot.
        var ready: u32 = 0;
        for (self.executors.items) |exec| {
            ready += @atomicLoad(u32, &exec.ready_count, .monotonic);
        }

        const blocked = if (total > ready) total - ready else 0;
        return .{ .total = total, .ready = ready, .blocked = blocked };
    }

    /// Dump per-fiber diagnostic info to `w`. One line per fiber, plus one
    /// line of resolved park-site source location for parked fibers when
    /// debug info is available:
    ///   fiber=<ptr> state=<tag> spawned=<file>:<line>
    ///     parked: <file>:<line>: 0x<addr> in <fn> (<module>)
    /// Acquires fibers_mutex for the duration of the dump.
    pub fn dumpFibers(self: *Runtime, w: *std.Io.Writer) std.Io.Writer.Error!void {
        self.fibers_mutex.lock();
        defer self.fibers_mutex.unlock();

        for (self.fibers.items) |t| {
            const tag = t.state.load(.acquire).tag;
            const tag_name: []const u8 = switch (tag) {
                .new => "new",
                .ready => "ready",
                .waiting => "waiting",
                .finished => "finished",
            };
            try w.print("fiber=0x{x} state={s}", .{ @intFromPtr(t), tag_name });
            if (!t.spawn_loc.isUnknown()) {
                try w.print(" spawned={s}:{d}", .{ t.spawn_loc.file, t.spawn_loc.line });
            }
            try w.writeByte('\n');
            const trace_len = std.mem.indexOfScalar(usize, &t.park_trace, 0) orelse t.park_trace.len;
            if (trace_len > 0) {
                try w.writeAll("  parked:\n");
                const st: std.debug.StackTrace = .{
                    .return_addresses = t.park_trace[0..trace_len],
                    .skipped = .none,
                };
                // writeStackTrace prints one symbolicated line per address.
                // On failure (no debug info), fall back to raw hex so callers
                // always see something.
                std.debug.writeStackTrace(&st, .{ .writer = w, .mode = .no_color }) catch {
                    for (t.park_trace[0..trace_len]) |a| try w.print("    0x{x}\n", .{a});
                };
            }
        }
    }

    /// Options for spawnWith. Future fields (deadline, name, affinity hint,
    /// etc.) extend this struct without growing the spawn API surface.
    pub const SpawnOptions = struct {
        /// Scheduling priority within the home executor's ready queues.
        priority: Priority = .normal,
        /// Home executor index. Null means use the runtime's round-robin policy.
        /// Must be < runtime.executorCount() if set; otherwise spawnWith returns
        /// error.ExecutorIndexOutOfRange.
        executor: ?usize = null,
        /// Optional spawn-site source location for diagnostics. Pass `@src()`
        /// at the call site to capture file/line for use by dumpFibers().
        spawn_loc: ?std.builtin.SourceLocation = null,
    };

    /// Spawn a task with explicit scheduling options.
    /// Returns error.ExecutorIndexOutOfRange if opts.executor is out of range,
    /// or error.RuntimeShutdown if the runtime is shutting down.
    pub fn spawnWith(
        self: *Runtime,
        opts: SpawnOptions,
        func: anytype,
        args: std.meta.ArgsTuple(@TypeOf(func)),
    ) !JoinHandle(meta.ReturnType(func)) {
        const Result = meta.ReturnType(func);
        const Args = @TypeOf(args);

        const Wrapper = struct {
            fn start(ctx: *const anyopaque, result: *anyopaque) void {
                const a: *const Args = @ptrCast(@alignCast(ctx));
                const r: *Result = @ptrCast(@alignCast(result));
                r.* = @call(.auto, func, a.*);
            }
        };

        const task = try spawnTask(
            self,
            @sizeOf(Result),
            .fromByteUnits(@alignOf(Result)),
            std.mem.asBytes(&args),
            .fromByteUnits(@alignOf(Args)),
            .{ .regular = &Wrapper.start },
            null,
            opts,
        );

        return JoinHandle(Result){
            .awaitable = &task.awaitable,
            .result = undefined,
        };
    }

    pub fn spawnBlocking(self: *Runtime, func: anytype, args: std.meta.ArgsTuple(@TypeOf(func))) !JoinHandle(meta.ReturnType(func)) {
        const Result = meta.ReturnType(func);
        const Args = @TypeOf(args);

        const Wrapper = struct {
            fn start(ctx: *const anyopaque, result: *anyopaque) void {
                const a: *const Args = @ptrCast(@alignCast(ctx));
                const r: *Result = @ptrCast(@alignCast(result));
                r.* = @call(.always_inline, func, a.*);
            }
        };

        const task = try spawnBlockingTask(
            self,
            @sizeOf(Result),
            .fromByteUnits(@alignOf(Result)),
            std.mem.asBytes(&args),
            .fromByteUnits(@alignOf(Args)),
            .{ .regular = &Wrapper.start },
            null,
        );

        return JoinHandle(Result){
            .awaitable = &task.awaitable,
            .result = undefined,
        };
    }

    /// Worker thread entry point. Initializes executor and runs until stopped.
    /// Signals worker.ready after initialization (success or failure).
    fn runWorker(self: *Runtime, worker: *Worker, id: u8) void {
        worker.executor.init(self, id) catch |e| {
            worker.err = e;
            worker.ready.set();
            return;
        };
        defer worker.executor.deinit();

        worker.ready.set();

        var backoff = Duration.fromMilliseconds(10);
        const max_backoff = Duration.fromMilliseconds(1000);

        while (true) {
            worker.executor.run(.until_stopped) catch |e| {
                if (self.shutting_down.load(.acquire)) break;
                log.err("Worker executor error: {}, retrying in {f}", .{ e, backoff });
                os.time.sleep(backoff);
                backoff = .{ .value = @min(backoff.value *| 2, max_backoff.value) };
                continue;
            };
            break;
        }
    }

    // Convenience methods that operate on the current coroutine context
    // These delegate to the current executor automatically
    // Most are no-op if not called from within a coroutine

    /// Cooperatively yield control to allow other tasks to run.
    /// The current task will be rescheduled and continue execution later.
    /// Can be called from the main thread or from within a coroutine.
    /// If called from a thread without an executor, yields the OS thread.
    /// Deprecated: use zio.yield() instead.
    pub fn yield(_: *Runtime) Cancelable!void {
        return mod.yield();
    }

    /// Sleep for the specified number of milliseconds.
    /// Returns error.Canceled if the task was canceled during sleep.
    /// Deprecated: use zio.sleep() instead.
    pub fn sleep(_: *Runtime, duration: Duration) Cancelable!void {
        return mod.sleep(duration);
    }

    /// Begin a cancellation shield to prevent being canceled during critical sections.
    /// Deprecated: use zio.beginShield() instead.
    pub fn beginShield(_: *Runtime) void {
        mod.beginShield();
    }

    /// End a cancellation shield.
    /// Deprecated: use zio.endShield() instead.
    pub fn endShield(_: *Runtime) void {
        mod.endShield();
    }

    /// Check if cancellation has been requested and return error.Canceled if so.
    /// This consumes the cancellation flag.
    /// Use this after endShield() to detect cancellation that occurred during the shielded section.
    /// Deprecated: use zio.checkCancel() instead.
    pub fn checkCancel(_: *Runtime) Cancelable!void {
        return mod.checkCancel();
    }

    /// Get the current monotonic timestamp.
    /// This uses the event loop's cached time for efficiency.
    /// Deprecated: use zio.now() instead.
    pub fn now(_: *Runtime) Timestamp {
        return mod.now();
    }

    /// Construct a `std.Io` instance backed by this runtime.
    pub fn io(self: *Runtime) std.Io {
        return @import("io.zig").fromRuntime(self);
    }

    /// Recover the `*Runtime` from a `std.Io` produced by `Runtime.io()`.
    pub fn fromIo(value: std.Io) *Runtime {
        return @import("io.zig").toRuntime(value);
    }
};

test "runtime: stats and dumpFibers" {
    const runtime = try Runtime.init(std.testing.allocator, .{});
    defer runtime.deinit();

    // No fibers yet
    {
        const s = runtime.stats();
        try std.testing.expectEqual(@as(u32, 0), s.total);
        try std.testing.expectEqual(@as(u32, 0), s.ready);
        try std.testing.expectEqual(@as(u32, 0), s.blocked);
    }

    const Body = struct {
        fn loop() Cancelable!void {
            // Park forever; cancellation unblocks us.
            while (true) try mod.sleep(.fromSeconds(60));
        }
    };

    var h1 = try runtime.spawnWith(.{ .spawn_loc = @src() }, Body.loop, .{});
    defer h1.cancel();
    var h2 = try runtime.spawnWith(.{ .spawn_loc = @src() }, Body.loop, .{});
    defer h2.cancel();

    // Yield so both fibers reach the park.
    try mod.sleep(.fromMilliseconds(10));

    {
        const s = runtime.stats();
        try std.testing.expectEqual(@as(u32, 2), s.total);
        try std.testing.expectEqual(@as(u32, 0), s.ready);
        try std.testing.expectEqual(@as(u32, 2), s.blocked);
    }

    // Dump should include this file path and a parked source line for each fiber.
    var buf: [8192]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try runtime.dumpFibers(&w);
    const out = w.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "runtime.zig") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "parked:") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "state=waiting") != null);
}

test "runtime: spawnBlocking smoke test" {
    const runtime = try Runtime.init(std.testing.allocator, .{
        .thread_pool = .{},
    });
    defer runtime.deinit();

    const blockingWork = struct {
        fn call(x: i32) i32 {
            return x * 2;
        }
    }.call;

    var handle = try runtime.spawnBlocking(blockingWork, .{21});
    defer handle.cancel();

    const result = handle.join();
    try std.testing.expectEqual(42, result);
}

test "Runtime: implicit run" {
    const runtime = try Runtime.init(std.testing.allocator, .{});
    defer runtime.deinit();

    const start = runtime.now();
    try std.testing.expect(start.value > 0);

    try runtime.sleep(.fromMilliseconds(10));

    const end = runtime.now();
    try std.testing.expect(end.value > start.value);
    try std.testing.expect(start.durationTo(end).toMilliseconds() >= 10);
}

test "Runtime: sleep from main" {
    const runtime = try Runtime.init(std.testing.allocator, .{});
    defer runtime.deinit();

    // Call sleep directly from main thread - no spawn needed
    const start = runtime.now();
    try runtime.sleep(.fromMilliseconds(10));
    const end = runtime.now();

    try std.testing.expect(end.value > start.value);
    try std.testing.expect(start.durationTo(end).toMilliseconds() >= 10);
}

test "runtime: basic sleep" {
    const runtime = try Runtime.init(std.testing.allocator, .{});
    defer runtime.deinit();

    try runtime.sleep(.fromMilliseconds(1));
}

test "runtime: now() returns monotonic time" {
    const runtime = try Runtime.init(std.testing.allocator, .{});
    defer runtime.deinit();

    const start = runtime.now();
    try std.testing.expect(start.value > 0);

    // Sleep to ensure time advances
    try runtime.sleep(.fromMilliseconds(10));

    const end = runtime.now();
    try std.testing.expect(end.value > start.value);
    try std.testing.expect(start.durationTo(end).toMilliseconds() >= 10);
}

test "runtime: sleep is cancelable" {
    const runtime = try Runtime.init(std.testing.allocator, .{});
    defer runtime.deinit();

    const sleepingTask = struct {
        fn call(rt: *Runtime) !void {
            // This will sleep for 1 second but should be canceled before completion
            try rt.sleep(.fromMilliseconds(1000));
            // Should not reach here
            return error.TestUnexpectedResult;
        }
    }.call;

    var timer = time.Stopwatch.start();

    var handle = try runtime.spawn(sleepingTask, .{runtime});
    defer handle.cancel();

    // Cancel the sleeping task
    handle.cancel();

    // Should return error.Canceled
    const result = handle.join();
    try std.testing.expectError(error.Canceled, result);

    // Ensure the sleep was canceled before completion
    try std.testing.expect(timer.read().toMilliseconds() <= 500);
}

test "runtime: shielded sleep is not cancelable" {
    const runtime = try Runtime.init(std.testing.allocator, .{});
    defer runtime.deinit();

    const shieldedSleepTask = struct {
        fn call(rt: *Runtime) !void {
            rt.beginShield();
            defer rt.endShield();
            // This sleep should complete even when canceled because it's shielded
            try rt.sleep(.fromMilliseconds(50));
        }
    }.call;

    var timer = time.Stopwatch.start();

    var handle = try runtime.spawn(shieldedSleepTask, .{runtime});
    defer handle.cancel();

    // Wait a bit to ensure the task is actually in the waiting state
    try runtime.sleep(.fromMilliseconds(10));

    // Try to cancel the sleeping task
    handle.cancel();

    // Should complete successfully (not canceled) because the sleep was shielded
    const result = handle.join();
    try std.testing.expectEqual({}, result);

    // Ensure the sleep completed (took at least 50ms)
    try std.testing.expect(timer.read().toMilliseconds() >= 40);
}

test "runtime: yield from main allows tasks to run" {
    const runtime = try Runtime.init(std.testing.allocator, .{});
    defer runtime.deinit();

    var counter: usize = 0;

    const yieldingTask = struct {
        fn call(counter_ptr: *usize) !void {
            for (0..10) |_| {
                counter_ptr.* += 1;
                try yield();
            }
        }
    }.call;

    var handle = try runtime.spawn(yieldingTask, .{&counter});
    defer handle.cancel();

    // Instead of join(), use yield() from main to let the task run
    var iterations: usize = 0;
    while (counter < 10) : (iterations += 1) {
        if (iterations >= 100) {
            std.debug.print("yield from main not working: counter={}, iterations={}\n", .{ counter, iterations });
            return error.TestExpectedEqual;
        }
        try yield();
    }

    try std.testing.expectEqual(10, counter);
}

test "runtime: sleep from main allows tasks to run" {
    const runtime = try Runtime.init(std.testing.allocator, .{});
    defer runtime.deinit();

    var counter: usize = 0;

    const yieldingTask = struct {
        fn call(counter_ptr: *usize) !void {
            for (0..10) |_| {
                counter_ptr.* += 1;
                try yield();
            }
        }
    }.call;

    var handle = try runtime.spawn(yieldingTask, .{&counter});
    defer handle.cancel();

    // Instead of join(), use sleep() from main to let the task run
    var iterations: usize = 0;
    while (counter < 10) : (iterations += 1) {
        if (iterations >= 100) {
            std.debug.print("sleep from main not working: counter={}, iterations={}\n", .{ counter, iterations });
            return error.TestExpectedEqual;
        }
        try runtime.sleep(.fromMilliseconds(1));
    }

    try std.testing.expectEqual(10, counter);
}

test "runtime: multi-threaded execution with 2 executors" {
    const runtime = try Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer runtime.deinit();

    const TestContext = struct {
        var counter: usize = 0;

        fn task(rt: *Runtime) !void {
            try rt.sleep(.fromMilliseconds(10));
            _ = @atomicRmw(usize, &counter, .Add, 1, .monotonic);
        }
    };

    TestContext.counter = 0;

    var group: Group = .init;
    defer group.cancel();

    for (0..4) |_| {
        try group.spawn(TestContext.task, .{runtime});
    }

    try group.wait();
    try std.testing.expect(!group.hasFailed());

    try std.testing.expectEqual(4, TestContext.counter);
}

test "runtime: multi-threaded execution with 64 executors" {
    const runtime = try Runtime.init(std.testing.allocator, .{ .executors = .exact(64) });
    defer runtime.deinit();

    try std.testing.expectEqual(64, runtime.executors.items.len);
}

test "runtime: spawnWith pins task to specific executor" {
    const runtime = try Runtime.init(std.testing.allocator, .{ .executors = .exact(4) });
    defer runtime.deinit();

    try std.testing.expectEqual(@as(usize, 4), runtime.executorCount());

    const TestFn = struct {
        fn check(expected_id: u6) !void {
            if (getCurrentExecutor().id != expected_id) return error.WrongExecutor;
        }
    };

    var i: usize = 0;
    while (i < runtime.executorCount()) : (i += 1) {
        const expected: u6 = @intCast(i);
        var handle = try runtime.spawnWith(.{ .executor = i }, TestFn.check, .{expected});
        try handle.join();
    }
}

test "runtime: spawnWith rejects out-of-range executor index" {
    const runtime = try Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer runtime.deinit();

    const noop = struct {
        fn run() void {}
    }.run;

    try std.testing.expectError(
        error.ExecutorIndexOutOfRange,
        runtime.spawnWith(.{ .executor = 2 }, noop, .{}),
    );
}

test "runtime: spawnWith executor and priority together" {
    const runtime = try Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer runtime.deinit();

    const State = struct {
        gate_open: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        order: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
        high_order: u32 = 0,
        high_executor_id: u8 = 0,
        normal_orders: [4]u32 = .{ 0, 0, 0, 0 },
        normal_executor_ids: [4]u8 = .{ 0, 0, 0, 0 },
    };
    var state: State = .{};

    const TestFn = struct {
        // Holds the pinned executor hostage until the main thread has finished
        // queuing every priority-bearing task. Busy-wait is deliberate: a
        // parking wait would release the executor to drain the queue eagerly,
        // and we want all 5 tasks present when getNextTask makes its choice.
        fn gateTask(s: *State) void {
            while (!s.gate_open.load(.acquire)) {
                std.atomic.spinLoopHint();
            }
        }
        fn normalTask(s: *State, order_slot: *u32, id_slot: *u8) void {
            id_slot.* = getCurrentExecutor().id;
            order_slot.* = s.order.fetchAdd(1, .monotonic) + 1;
        }
        fn highTask(s: *State) void {
            s.high_executor_id = getCurrentExecutor().id;
            s.high_order = s.order.fetchAdd(1, .monotonic) + 1;
        }
    };

    const pinned_exec: usize = 1;

    // Pin the gate to executor 1 so the worker is busy while we queue the rest.
    var gate = try runtime.spawnWith(.{ .executor = pinned_exec }, TestFn.gateTask, .{&state});

    var normals: [4]JoinHandle(void) = undefined;
    for (&normals, &state.normal_orders, &state.normal_executor_ids) |*h, *order_slot, *id_slot| {
        h.* = try runtime.spawnWith(
            .{ .executor = pinned_exec },
            TestFn.normalTask,
            .{ &state, order_slot, id_slot },
        );
    }
    var high = try runtime.spawnWith(
        .{ .executor = pinned_exec, .priority = .high },
        TestFn.highTask,
        .{&state},
    );

    // All 5 priority-bearing tasks are now in executor 1's remote queue;
    // release the gate so it can drain them in priority order.
    state.gate_open.store(true, .release);

    gate.join();
    high.join();
    for (&normals) |*h| h.join();

    // All 5 priority-bearing tasks must have landed on the pinned executor.
    try std.testing.expectEqual(@as(u8, pinned_exec), state.high_executor_id);
    for (state.normal_executor_ids) |id| {
        try std.testing.expectEqual(@as(u8, pinned_exec), id);
    }
    // High must have been served first among the 5 (the gate doesn't touch order).
    try std.testing.expectEqual(@as(u32, 1), state.high_order);
    for (state.normal_orders) |o| {
        try std.testing.expect(o > state.high_order);
    }
}

test "runtime: spawnWith priority high runs before queued normals" {
    const runtime = try Runtime.init(std.testing.allocator, .{ .executors = .exact(1) });
    defer runtime.deinit();

    const State = struct {
        order: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
        high_order: u32 = 0,
        normal_orders: [4]u32 = .{ 0, 0, 0, 0 },
    };
    var state: State = .{};

    const TestFn = struct {
        fn normalTask(s: *State, slot: *u32) void {
            slot.* = s.order.fetchAdd(1, .monotonic) + 1;
        }
        fn highTask(s: *State) void {
            s.high_order = s.order.fetchAdd(1, .monotonic) + 1;
        }
    };

    // Spawn the normals first so they're all queued before the high task arrives.
    var normals: [4]JoinHandle(void) = undefined;
    for (&normals, &state.normal_orders) |*h, *slot| {
        h.* = try runtime.spawn(TestFn.normalTask, .{ &state, slot });
    }
    var high = try runtime.spawnWith(.{ .priority = .high }, TestFn.highTask, .{&state});

    high.join();
    for (&normals) |*h| h.join();

    // High must have been the first task served on its executor.
    try std.testing.expectEqual(@as(u32, 1), state.high_order);
    for (state.normal_orders) |o| {
        try std.testing.expect(o > state.high_order);
    }
}

test "Runtime: multi-threaded with task migration" {
    const runtime = try Runtime.init(std.testing.allocator, .{ .executors = .exact(8) });
    defer runtime.deinit();

    const ResetEvent = @import("sync/ResetEvent.zig");

    const TestContext = struct {
        group: *Group,
        done: ResetEvent = .{},
        counter: std.atomic.Value(u32) = .init(0),

        fn task(ctx: *@This(), parent: *ResetEvent) !void {
            parent.set();

            const n = ctx.counter.fetchAdd(1, .acquire);
            if (n >= 99) {
                ctx.done.set();
                return;
            }

            var event: ResetEvent = .{};
            ctx.group.spawn(task, .{ ctx, &event }) catch |err| {
                std.debug.print("task migration failed: {}\n", .{err});
                return err;
            };
            event.wait() catch |err| {
                std.debug.print("event wait failed: {}\n", .{err});
                return err;
            };
        }
    };

    var group: Group = .init;
    defer group.cancel();

    var ctx: TestContext = .{ .group = &group };

    var event: ResetEvent = .{};

    try group.spawn(TestContext.task, .{ &ctx, &event });

    try ctx.done.wait();

    try group.wait();
    try std.testing.expect(!group.hasFailed());

    try std.testing.expectEqual(100, ctx.counter.load(.acquire));
}

test "runtime: wake-before-park awaken bit stress (single executor)" {
    try wakeBeforeParkStress(1);
}

test "runtime: wake-before-park awaken bit stress (two executors)" {
    try wakeBeforeParkStress(2);
}

fn wakeBeforeParkStress(executor_count: u8) !void {
    const ResetEvent = @import("sync/ResetEvent.zig");

    const runtime = try Runtime.init(std.testing.allocator, .{
        .executors = .exact(executor_count),
    });
    defer runtime.deinit();

    const Ctx = struct {
        // Number of ping-pong iterations to exercise the wake-before-park window.
        const iterations: u32 = 10_000;

        ping: ResetEvent = .{},
        pong: ResetEvent = .{},
        counter: std.atomic.Value(u32) = .init(0),

        // Waits on ping each iteration — this is the task that parks, and the one
        // whose awaken bit gets set when the waker fires between the condition check
        // and the actual park CAS in processCleanup.park.
        fn parker(ctx: *@This()) !void {
            for (0..iterations) |_| {
                try ctx.ping.wait();
                ctx.ping.reset();
                _ = ctx.counter.fetchAdd(1, .release);
                ctx.pong.set();
            }
        }

        // Fires ping immediately each iteration, without waiting for parker to park first.
        // With two executors this races directly with the park CAS, exercising the path
        // where scheduleTask sets awaken=true on a .ready task and processCleanup.park
        // consumes the token instead of transitioning to .waiting.
        fn waker(ctx: *@This()) !void {
            for (0..iterations) |_| {
                ctx.ping.set();
                try ctx.pong.wait();
                ctx.pong.reset();
            }
        }
    };

    var ctx: Ctx = .{};
    var group: Group = .init;
    defer group.cancel();

    try group.spawn(Ctx.parker, .{&ctx});
    try group.spawn(Ctx.waker, .{&ctx});

    try group.wait();
    try std.testing.expect(!group.hasFailed());
    // Any lost wake would cause parker to hang in ping.wait() forever — group.wait()
    // would never return. The counter confirms all iterations ran to completion.
    try std.testing.expectEqual(Ctx.iterations, ctx.counter.load(.acquire));
}

test "runtime: mutex contention with task migration" {
    const Mutex = @import("sync/Mutex.zig");

    const runtime = try Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer runtime.deinit();

    var mutex: Mutex = .init;
    var counter: u32 = 0;

    const Worker = struct {
        fn run(m: *Mutex, c: *u32) !void {
            for (0..1_000) |_| {
                try m.lock();
                defer m.unlock();
                c.* += 1;
            }
        }
    };

    var group: Group = .init;
    defer group.cancel();

    try group.spawn(Worker.run, .{ &mutex, &counter });
    try group.spawn(Worker.run, .{ &mutex, &counter });

    try group.wait();
    try std.testing.expect(!group.hasFailed());
    try std.testing.expectEqual(2_000, counter);
}
