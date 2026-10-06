# Developer guide

kernmini is one Rust kernel engine with two language boundaries: native Rust traits and a separate PyO3 binding crate. There is no separate Python protocol engine.

## Development setup

In a uv workspace, run `ws-sync` after cloning or changing dependency metadata. Rebuild the editable extension after Rust changes:

```bash
cargo develop
pytest -q
```

`Cargo.toml` is the version source. The published `kernmini` crate has no Python dependency. The unpublished `kernmini-py` crate in `py/` builds `kernmini._native`. `cargo develop` and bare `cargo test` share the ordinary library builds; unit tests compile a separate `cfg(test)` executable.

## Rust architecture

The engine owns connection loading, ZMTP transport, HMAC-signed Jupyter messages, duplicate-signature rejection, shell/control routing, IOPub, stdin, heartbeat, execution scheduling, interruption, subshells, and shutdown.

Synchronous interpreters can use `ThreadWorker` without another language trait. It owns only thread startup, closure dispatch, replies and shutdown; language adapters retain interruption and all language semantics. Rustygate's Luau adapter and miniapl use it.

Router and heartbeat peers are independent connection tasks. Ordinary peer EOF removes any router registration and ends silently; genuine I/O, handshake, and protocol failures are reported on kernel stderr.

The language boundary has two levels:

- `Language` supplies the parent `LanguageSession` and creates independent child sessions.
- `LanguageSession` supplies kernel metadata, execution, completion, inspection, completeness, history, comms, debugging, and shutdown.

A `LanguageSession` is also a `Language` with that one session and no subshells, so a language without subshells passes its session straight to `run_kernel`. `run_kernel_blocking` runs a kernel on its own Tokio runtime, for a program that has none, after awaiting the future that starts its language.

### Errors

Public operations and language traits return `kernmini::Result<T>`. Its `Error` has an inspectable `kind()`, a diagnostic, and a standard `source()` chain. `context(...)` adds a diagnostic without changing the kind; `caused_by(...)` retains an underlying error. Errors are cloneable so one connection failure can complete several pending requests without losing its cause.

The kinds are `Interrupted`, `Closed`, `TimedOut`, `Unavailable`, `InvalidInput`, `Io`, `Protocol`, and `Adapter`. They describe the failure, not a universal recovery policy: a lost client and a stopped interpreter both close a resource but require different actions. A timeout does not imply that a remote operation was never performed. Use `Error::adapter(error)` for a foreign language implementation's operational failure; non-Send interpreter errors must become owned diagnostics before crossing a thread boundary.

An executed program's exception is still a `LanguageError` inside `Ok(ExecuteOutcome)`, not an operational `Err`. `WireError` remains the detailed Jupyter codec error and is retained as the cause of higher-level protocol errors. Failed DAP responses remain response values, not transport errors.

The Python boundary maps interruption to `KeyboardInterrupt`, timeouts to `TimeoutError`, invalid input to `ValueError`, and I/O failures to `OSError` (with the OS error number when available). Other operational errors raise `kernmini.KernelError`, a `RuntimeError` subclass. Converted exceptions carry a `kind` attribute with the Rust kind's name, such as `Closed` or `Protocol`; underlying Python exceptions are retained as causes.

Ordinary adapter request failures produce an error reply and idle rather than silently ending the shell. A closed language service is fatal and reaches the kernel runner. Listener, shell-task, interrupt-handler, and output-pump failures also reach their owner. A client's failed reply connection does not terminate the kernel. Output submission still means enqueueing, not subscriber acknowledgement.

Each shell session is driven by one scheduler object which owns its queue, active execution, hold, and interruption state. Shared transport and language handles live in its services object. Output from executions and comm handlers uses the same event pump, so stream, display, buffer, flush, and parent-routing behavior cannot diverge between the two paths.

An execute receives an `ExecutionContext`. It emits streams and displays, requests stdin, publishes arbitrary messages, observes or registers for interruption, and opens subshell routes. The engine converts these events into correctly parented Jupyter messages.

`run_kernel` installs Tokio SIGINT handling. `run_kernel_with_interrupter` lets an embedding host supply its own `KernelInterrupter`.

Kernel developers choose the Jupyter interrupt mechanism in their kernelspec. Set `"interrupt_mode": "message"` to receive `interrupt_request` on the control channel; omitting it selects signal mode. Both routes use the same language interruption handler on Unix. On Windows, use message mode: kernmini does not watch Jupyter's `JPY_INTERRUPT_EVENT` handle. The Python shell's optional native `interrupt()` hook works with message mode on every platform.

`DapClient` is independent of the kernel engine and reusable by any language adapter. It owns DAP's `Content-Length` TCP framing, sequence allocation, pending responses, timeouts, asynchronous events, and connection teardown. The language adapter owns debugger startup and language-specific request handling.

`install_kernelspec` writes a `kernel.json`. `install_kernelspec_dir` copies a kernelspec directory. Both replace any kernelspec of the same name in `share/jupyter/kernels` under a prefix, or else in the user Jupyter data directory. `JUPYTER_DATA_DIR` overrides the user directory. The Python functions of the same names wrap them. Their `user` parameter is accepted and ignored.

## Python adapter

The public Python `kernmini.run_kernel(connection_file, shell_factory)` is synchronous. It uses loopmini when available, falls back to the standard asyncio loop, and accepts an explicit `loop_factory`. `_native.run_kernel` is the underlying awaitable used by the wrapper.

The factory takes no arguments and may return a shell directly or an awaitable shell. It runs on each session's owning loop, creating the parent shell once and a new shell on each child session. Initialization completes before that session serves requests. Shared language state belongs in the factory closure, as ipymini does for its namespace.

A Python shell provides:

- `kernel_info()`: implementation, version, banner, and `language_info`.
- `execute(code, silent=, store_history=, user_expressions=, allow_stdin=, execution_count=)`: an awaitable returning optional `result`, `result_metadata`, `error`, `user_expressions`, and `payload`.

The adapter uses optional capabilities when present:

- `set_stream_sender(sender)` and `set_display_sender(sender)` install live output callbacks.
- `set_input_sender(sender)` installs blocking `(prompt, password) -> str` input routing.
- `bind_kernel(kernel)` exposes the small kernel proxy expected by IPython integrations.
- `execution_context(allow_stdin=, silent=)` wraps execution capture.
- `output_context()` wraps output from comm handlers.
- `complete`, `inspect`, `is_complete`, and `history` provide language services. They may be synchronous or awaitable; omitted services use the same default replies as Rust sessions.
- `debug_request` and a `debugger.event_callback` provide language-specific DAP integration; `kernmini._native.DapClient` is the optional shared transport.
- `comm_info` and `message` provide the language's comm manager and incoming comm dispatch. Both may be synchronous or awaitable. Kernmini knows nothing about IPython or ipymini comm objects.
- `comm_manager`, when exposed by the shell, is available through the kernel proxy passed to `bind_kernel`.
- `interrupt()` is a synchronous native-language interrupt hook, called directly on the control thread instead of cancelling Python execution. It must be thread-safe and return promptly; it should signal the interpreter, not queue behind its execution.
- `shutdown()` runs on the shell's owning loop before that loop stops. It may be synchronous or awaitable.

Python adapters for synchronous native interpreters can use the same `ThreadWorker` as Rust:

```python
from kernmini import ThreadWorker

worker = await ThreadWorker.start(create_interpreter, stack_size=64*1024*1024)
result = await worker.call(lambda interpreter: interpreter.execute(code))
await worker.shutdown()
```

The factory and callbacks run on one dedicated thread; shutdown releases the worker's interpreter reference on that thread. `name=` names the thread; `stack_size=` is in bytes and defaults to the OS thread default. Each callback carries its caller's contextvars, so stream, display and stdin callbacks retain the active execution's routing. Callbacks are synchronous; cancelling the awaiting task does not stop an in-progress native call. A shell's `shutdown()` should explicitly free native resources through `worker.call(...)`, then await `worker.shutdown()`.

The parent shell runs on a persistent asyncio loop in the Python main thread. Child shells run on supervised OS threads with their own persistent loops created by the same factory. Both use the same loop lifecycle: the loop is current while the session runs, and shutdown closes it through `asyncio.Runner`. Kernmini's multi-thread Tokio runtime independently drives transport, queues, output, control, and interrupt futures, so synchronous Python cannot block the engine.

A `SystemExit` raised inside a task leaves the loop rather than the task, so `kernmini._bridge.run_loop` drives every loop and re-enters it: user code cannot end the kernel.

`pyo3-async-runtimes` bridges Python awaitables onto their owning loop. Without a shell `interrupt()` hook, interrupts cancel async cells through that loop and inject `KeyboardInterrupt` into synchronous Python. Arbitrary C code cannot be interrupted safely without its own interrupt API; native adapters use the hook to call that API.

## Execution and concurrency

Each language session owns a serial execute queue. Completion, inspection, history, debugging, comms, and control requests remain responsive while a cell runs.

Each session has one execution count, which the shell scheduler keeps. It starts at 1. An execute that stores history and isn't silent takes the count and advances it. Every other execute, and every reply that reports a count, uses the count unchanged. This is IPython's numbering. A language reads the count of its execution from `ExecutionContext::execution_count`, for its own `execute_result` messages and history. The Python adapter passes it to `execute` as `execution_count`, and ipymini sets IPython's counter from it.

Two execute metadata extensions are supported:

- `priority` is numeric and defaults to zero. Higher-priority queued cells run first; active execution is never preempted.
- `hold: true` emits `execute_input` and parks the queue until a control `release_request` arrives. Strictly higher-priority work may pass the hold. An error release or interrupt engages ordinary stop-on-error behavior.

`KERNMINI_HOLD_TIMEOUT` is the hold backstop in seconds and defaults to 3600.

An explicit `subshell_id` on a shell request creates that named subshell when missing, then routes the request there. `create_subshell_request` also accepts an optional `subshell_id`; a supplied ID makes explicit creation idempotent. Python code can use `kernmini.subshell()` to route later requests from its client session through a temporary child, or `kernmini.sidecar()` to route through the persistent named `sidecar` subshell.

## Output and stdin

`ExecutionContext` is the single output boundary. Native async producers call `emit(LanguageEvent).await`; producers on synchronous execution threads call `emit_blocking(LanguageEvent)`. Both return an error if the output channel has closed. The Python callbacks release the GIL while sending. The Rust engine associates every stream, display, buffer, stdin request, and arbitrary published message with its execution before sending it over IOPub or stdin. The PyO3 adapter keeps the current context in a ContextVar so Python callbacks and IPython comm handlers reach the correct sink.

`KERNMINI_IOPUB_QMAX` bounds each execution's event queue and each IOPub peer's outgoing queue, in messages, and defaults to 10000. Full queues wait for space instead of dropping events; a slow direct IOPub subscriber can slow execution. This is not an output-byte limit. Environment configuration is read once when the kernel starts and shared by its parent and child sessions.

The existing output pump drains at most one queue capacity per batch and joins adjacent same-name stream events with `String::push_str`. It never waits to fill a batch or merges across stream-name changes, displays, input, or flush markers. Flush preserves event order through publication; it does not acknowledge subscriber receipt.

`input()` returns `Interrupted` when its execution is cancelled, including during shutdown. Cancellation is recorded before waking input, and applies while waiting for the stdin peer, sending the prompt, or awaiting its reply. A lost stdin peer instead returns `Closed`; an empty reply is a successful empty string. Input forbidden by `allow_stdin` or requested outside execution returns `Unavailable`. These errors do not depend on the timing of a separate language interrupt handler. A gateway's frontend disconnect is not necessarily a kernel-peer disconnect; that policy remains with the gateway.

## Lifecycle

On Unix, the Python wrapper may place a standalone kernel in its own process group before initializing its shell factory. On shutdown it terminates that group after protocol cleanup so user-created subprocesses do not survive the kernel. It captures the original parent PID before initialization and watches it once the engine starts. Embedders can pass `own_process_group=False` to avoid changing or terminating their host process group.

On Windows, `own_process_group` has no effect. Protocol shutdown and language cleanup still run, but kernmini does not monitor parent death or terminate descendant processes. Windows wheels are built for x64 and ARM64 on CPython 3.11–3.14; integration tests run on Linux.

`LanguageSession::shutdown()` is the asynchronous language lifecycle boundary. Child Python sessions stop their loop and join their thread without blocking a Tokio worker. `Drop` only requests cleanup for exceptional paths.

## Tests

`pytest -q` contains three readable end-to-end stories:

- a Python echo shell through the public PyO3 runner;
- a pure Rust echo language, implemented entirely in the example binary, through the crate API;
- an IPython shell through the Python adapter.

`ConKernelClient` launches each kernel and manages its Jupyter requests, replies, IOPub messages, stdin, and shutdown. Tests use live protocol events to synchronize concurrent behavior rather than sleeps or hand-written socket draining. Standalone Rust tests cover wire framing and language interruption primitives. A Python integration test drives a real debugpy session through the DAP transport directly, since that transport is the subject of the test. ipymini's complete protocol and behavior suite is kernmini's main integration test.

```bash
cd ../ipymini
pytest -q
```

Run `cargo test` for the pure Rust surface and `chkstyle` after Python edits.
