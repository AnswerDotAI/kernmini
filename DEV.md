# Developer guide

kernmini is one Rust kernel engine with two language boundaries: native Rust traits and a feature-gated PyO3 adapter. There is no separate Python protocol engine.

## Development setup

Kernmini is built by Maturin. In a uv workspace, run `uv sync` after cloning or changing dependency metadata. Rebuild the editable extension after Rust changes:

```bash
maturin develop
pytest -q
```

`Cargo.toml` is the version source. The default crate is a reusable `rlib`; Maturin enables `extension-module` to build `kernmini._native`.

## Rust architecture

The engine owns connection loading, ZMTP transport, HMAC-signed Jupyter messages, duplicate-signature rejection, shell/control routing, IOPub, stdin, heartbeat, execution scheduling, interruption, subshells, and shutdown.

The language boundary has two levels:

- `Language` supplies the parent `LanguageSession` and creates independent child sessions.
- `LanguageSession` supplies kernel metadata, execution, completion, inspection, completeness, history, comms, debugging, and shutdown.

Each shell session is driven by one scheduler object which owns its queue, active executions, hold, lock, and interruption state. Shared transport and language handles live in its services object. Output from executions and comm handlers uses the same event pump, so stream, display, buffer, flush, and parent-routing behavior cannot diverge between the two paths.

An execute receives an `ExecutionContext`. It emits streams and displays, requests stdin, publishes arbitrary messages, observes or registers for interruption, releases the execution queue with `unlock()`, and opens temporary subshell routes. The engine converts these events into correctly parented Jupyter messages.

`run_kernel` installs Tokio SIGINT handling. `run_kernel_with_interrupter` lets an embedding host supply its own `KernelInterrupter`.

`DapClient` is independent of the kernel engine and reusable by any language adapter. It owns DAP's `Content-Length` TCP framing, sequence allocation, pending responses, timeouts, asynchronous events, and connection teardown. The language adapter owns debugger startup and language-specific request handling.

## Python adapter

The public Python `kernmini.run_kernel(connection_file, shell_factory)` is synchronous. It uses loopmini when available, falls back to the standard asyncio loop, and accepts an explicit `loop_factory`. `_native.run_kernel` is the underlying awaitable used by the wrapper.

The factory takes no arguments. It creates the parent shell once and a new shell on each child session. Shared language state belongs in the factory closure, as ipymini does for its namespace.

A Python shell provides:

- `execution_count`: current integer execution count.
- `kernel_info()`: implementation, version, banner, and `language_info`.
- `execute(code, silent=, store_history=, user_expressions=, allow_stdin=)`: an awaitable returning `execution_count` and optional `result`, `result_metadata`, `error`, `user_expressions`, and `payload`.

The adapter uses optional capabilities when present:

- `set_stream_sender(sender)` and `set_display_sender(sender)` install live output callbacks.
- `set_input_sender(sender)` installs blocking `(prompt, password) -> str` input routing.
- `bind_kernel(kernel)` exposes the small kernel proxy expected by IPython integrations.
- `execution_context(allow_stdin=, silent=)` wraps execution capture.
- `output_context()` wraps output from comm handlers.
- `complete`, `inspect`, `is_complete`, and `history` provide language services.
- `debug_request` and a `debugger.event_callback` provide language-specific DAP integration; `kernmini._native.DapClient` is the optional shared transport.
- `comm_info` and `message` provide the language's comm manager and incoming comm dispatch. Kernmini knows nothing about IPython or ipymini comm objects.
- `comm_manager`, when exposed by the shell, is available through the kernel proxy passed to `bind_kernel`.

The parent shell runs on a persistent asyncio loop in the Python main thread. Child shells run on supervised OS threads with their own persistent loops created by the same factory. Kernmini's multi-thread Tokio runtime independently drives transport, queues, output, control, and interrupt futures, so synchronous Python cannot block the engine.

`pyo3-async-runtimes` bridges Python awaitables onto their owning loop. Interrupts cancel async cells through that loop and inject `KeyboardInterrupt` into synchronous Python. A child blocked indefinitely in arbitrary C code cannot be interrupted safely; kernmini does not pretend otherwise.

## Execution and concurrency

Each language session owns a serial execute queue. Completion, inspection, history, debugging, comms, and control requests remain responsive while a cell runs.

Two execute metadata extensions are supported:

- `priority` is numeric and defaults to zero. Higher-priority queued cells run first; active execution is never preempted.
- `hold: true` emits `execute_input` and parks the queue until a control `release_request` arrives. Strictly higher-priority work may pass the hold. An error release or interrupt engages ordinary stop-on-error behavior.

`KERNMINI_HOLD_TIMEOUT` is the hold backstop in seconds and defaults to 3600.

Python code can call `kernmini.unlock()` to release its queue baton while the current cell continues, or use `kernmini.subshell()` to route later requests from that client session through a temporary child.

## Output and stdin

`ExecutionContext` is the single output boundary. The Rust engine associates every stream, display, buffer, stdin request, and arbitrary published message with its execution before sending it over IOPub or stdin. The PyO3 adapter keeps the current context in a ContextVar so Python callbacks and IPython comm handlers reach the correct sink.

`KERNMINI_IOPUB_QMAX` controls the bounded Rust IOPub queue and defaults to 10000.
Environment configuration is read once when the kernel starts and shared by its parent and child sessions.

## Lifecycle

The Python wrapper may place a standalone kernel in its own process group. On shutdown it terminates that group after protocol cleanup so user-created subprocesses do not survive the kernel. It also watches the original parent PID. Embedders can pass `own_process_group=False` to avoid changing or terminating their host process group.

`LanguageSession::shutdown()` is the asynchronous language lifecycle boundary. Child Python sessions stop their loop and join their thread without blocking a Tokio worker. `Drop` only requests cleanup for exceptional paths.

## Tests

`pytest -q` contains three readable end-to-end stories:

- a Python echo shell through the public PyO3 runner;
- a pure Rust echo language, implemented entirely in the example binary, through the crate API;
- an IPython shell through the Python adapter.

Standalone Rust tests cover wire framing and language interruption primitives. A Python integration test drives a real debugpy session through the DAP
transport. ipymini's complete protocol and behavior suite is kernmini's main integration test.

```bash
cd ../ipymini
pytest -q
```

Run `cargo test` for the pure Rust surface and `chkstyle` after Python edits.
