# Developer guide

kernmini is the language-agnostic core extracted from `ipymini` (see its DEV.md for the full architecture prose: kernel object graph, life of an execute_request, IOPub semantics, interrupts, startup/shutdown -- all of that machinery now lives here, and that document remains its best narrative description). This guide covers what is specific to kernmini as a standalone package: the shell contract, the capability gates, and the boundaries of the Session copy.

## The shell contract

`MiniKernel(connection_file, shell_factory, ...)` builds one shell per subshell by calling `shell_factory(**kw)` with: `request_input` (callable `(prompt, password) -> str`, routed through the stdin thread), `debug_event_callback`, `zmq_context`, `user_ns` (dict shared across subshells), `use_singleton` (True for the parent subshell), `exec_scopes` (a `microio.ScopeGroup` the kernel cancels on interrupt -- register async work with it to make it interruptible), and `sync_execution_context` (a context manager to wrap synchronous execution so interrupts know to inject rather than cancel). A simple shell accepts what it needs and `**kw`s the rest.

Required members:

- `execute(code, silent=, store_history=, user_expressions=, allow_stdin=)` -- awaitable; returns a dict with `execution_count`, and optionally `result` (mime bundle) + `result_metadata`, `error` (`ename`/`evalue`/`traceback`), `user_expressions`, `payload`. Error content is entirely the shell's: each language names its own errors. An `ename` of `KeyboardInterrupt` (or `CancelledError` while an interrupt is in flight) marks the cell aborted.
- `execution_count` -- int property, read before execution for `execute_input` and for reply defaults.
- `execution_context(allow_stdin, silent)` -- context manager entered around `execute`; bind per-request IO/capture state here.
- `set_stream_sender(sender)` -- the kernel hands the shell a `(name, text)` callback publishing IOPub `stream` messages parented to the active request.

Optional members, each a capability the kernel detects with `getattr`:

- `set_display_sender(sender)` -- callback for display events (`display_data`/`update_display_data`/`clear_output` dicts).
- `complete`/`inspect`/`is_complete` -- language services; missing ones get spec-shaped empty replies.
- `history(...)` -- history_request; absent means empty history.
- `debug_request(request_json)` -- DAP bridging; presence advertises `debugger` in kernel_info.
- `interrupt()` -- full responsibility for interrupting the current execute, however the shell runs it (e.g. a break flag written while the engine blocks in C, polled between statements). Without it, the kernel cancels async executes through `exec_scopes` and injects `KeyboardInterrupt` into the subshell thread via `PyThreadState_SetAsyncExc` for sync ones -- defaults that assume the shell executes Python bytecode.
- `kernel_info()` -- the shell's contribution to kernel_info_reply (`implementation`, `implementation_version`, `banner`, `language_info`); identity belongs to the language layer, and it can ask the live runtime (e.g. jkernel queries J for its version).
- `bind_kernel(kernel)` -- called once per shell at construction; ipymini uses it to set `get_ipython().kernel` and bind the process-global comm layer.
- `output_context()` -- context manager for attributing out-of-band handler output (comms); required only when a `comm_manager` is passed.

`MiniKernel` kwargs: `comm_manager` (a `comm`-package-style manager; None disables comm handling), `subshells` (False refuses `create_subshell_request` cleanly -- for single-threaded runtimes), `terminate_process_group`.

## Priority and holds

Two execute-metadata extensions, both ours (no Jupyter frontend sends them; unaware kernels and clients are unaffected):

- `priority` (int, default 0): each subshell's cell queue is a `microio.PriorityMailbox` -- highest priority first, FIFO within a level. A request already started is never preempted; priority only reorders what is still queued.
- `hold` (true): the request emits `execute_input` and then parks instead of executing, holding the queue while an external activity (for solveit, an AI prompt turn) runs elsewhere. While parked, only strictly-higher-priority requests are serviced (the mailbox `floor`). It completes on a `release_request` control message (`{msg_id, status}`; `status: "error"` makes the reply an error, engaging normal `stop_on_error` tail-abort), on interrupt (as `KeyboardInterrupt`, like any cell), or on the `KERNMINI_HOLD_TIMEOUT` backstop. `release_reply` carries `found`: releasing a hold that already completed is a quiet no-op, since the timeout race makes late releases legitimate.

Aborts and the priority queue: the stop-on-error fence (`subshell_abort_clear`) marks a *position*, and the heap has none, so the fence is consumed by the mailbox `gate` -- a hook that sees every item in channel arrival order as it leaves the channel, before the heap can reorder it. The gate also aborts executes arriving inside the abort window, which keeps the contract that a client who has *seen* the error reply can submit again immediately, while stragglers sent before it abort.

## Session

`session.MiniSession` is [jupywire](https://github.com/AnswerDotAI/jupywire)'s `Session` (the shared trimmed mirror of `jupyter_client.Session`; BSD attribution and the wire-compatibility story live there) plus the zmq socket halves (`send`/`recv`) and kernel defaults: unsigned unless a key is given, username "kernel". Cross-verification against jupyter_client moved to jupywire's tests; `tests/test_session.py` here keeps the subclass checks, and ipymini's compat suite proves the wire against unpatched real clients. The "Duplicate Signature" ValueError text is load-bearing: the routers match it to drop replays silently.

## Env vars

`KERNMINI_DEBUG`, `KERNMINI_DEBUG_MSGS`, `KERNMINI_CELL_NAME`, `KERNMINI_IOPUB_QMAX`, `KERNMINI_IOPUB_SNDHWM`, `KERNMINI_IOPUB_XPUB`, `KERNMINI_STOP_ON_ERROR_TIMEOUT` -- semantics as documented in ipymini's DEV.md (renamed from `IPYMINI_*` when the code moved here), plus `KERNMINI_HOLD_TIMEOUT`: seconds before a parked hold completes as a `HoldTimeout` error (default 3600), the backstop for a client that died owing a release.

## Tests

`pytest -q` runs the unit tests (zmqthread, session, debug infra) plus `tests/test_kernel_echo.py`: a complete kernel built on a trivial echo shell, driven over real sockets by `MiniSession` -- the proof that no IPython is needed. The heavy integration coverage lives deliberately in ipymini's suite (`tests/kernel/`, `tests/compat/`), which exercises this package through a real IPython kernel and unpatched jupyter_client; treat ipymini green as part of kernmini's definition of done. Downstream kernels: ipymini (Python/IPython), jnb (J), aplnb (APL, planned).

## Style and releases

fastai style (`chkstyle` before committing). Releases via fastship (`ship-changelog` / `ship-release`); the tree carries the next version.
