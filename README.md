# kernmini

Everything a Jupyter kernel needs except the language.

kernmini is a Rust engine for Jupyter kernels. It owns ZMTP transport, signed Jupyter messages, shell and control routing, IOPub, stdin, heartbeat, execution queues, interrupts, JEP 91 subshells, debugging transport, and process lifecycle. A language supplies execution, completion, inspection, history, and kernel metadata through a small adapter.

[ipymini](https://github.com/AnswerDotAI/ipymini) is the reference Python kernel. It uses IPython for Python semantics and kernmini for the kernel protocol.

## A complete Python kernel

```python
import sys
from kernmini import run_kernel


class EchoShell:
    def __init__(self):
        self.stream = None

    def set_stream_sender(self, sender): self.stream = sender

    def kernel_info(self):
        return dict(implementation="echo", implementation_version="0.1", banner="echo",
            language_info=dict(name="echo", version="0.1", mimetype="text/plain", file_extension=".txt"))

    async def execute(self, code, **kwargs):
        if self.stream: self.stream("stdout", f"echo: {code}\n")
        return dict(result={"text/plain": code.upper()})


run_kernel(sys.argv[-1], EchoShell, own_process_group=True)
```

`run_kernel` creates a persistent asyncio event loop and runs the Rust engine until shutdown. It uses loopmini when installed and the standard asyncio loop otherwise; `loop_factory=` can select one explicitly. The shell factory can be synchronous or asynchronous, and initialization completes before serving requests. The factory is also used to create independent language sessions for JEP 91 subshells. Standalone executables can request process-group ownership, while embedded kernels leave their host process group unchanged by default.

Rust language implementations use the `Language` and `LanguageSession` traits directly. A session with no subshells is a `Language` by itself. `run_kernel_blocking` serves one from a program with no Tokio runtime. `ExecutionContext` provides stream, display, stdin, interrupt, and subshell routing without exposing Jupyter transport details. kernmini keeps each session's execution count, and the context gives each execution its count.

Operations return `kernmini::Result<T>` with a typed `ErrorKind` and preserved causes. These operational failures are separate from an executed program's `LanguageError`. See [the error contract](DEV.md#errors) for cancellation, closure, timeout, and Python exception mappings.

`ThreadWorker` lets async adapters call a synchronous interpreter on a dedicated thread. Its factory creates the interpreter there and `call` awaits a closure's result. Rust interpreters need not be `Send`; adapters supply a thread builder for its name and stack size, and `shutdown` waits for destruction on the same thread. Python uses `await ThreadWorker.start(factory, stack_size=...)`, `await worker.call(callback)` and `await worker.shutdown()`; shutdown releases the worker's interpreter reference there. Python shells may supply `interrupt()` to signal a native interpreter from the control thread, and `shutdown()` to explicitly free native resources through `worker.call(...)` before shutting down the worker. Language traits stay async.

Output queues are bounded and apply backpressure rather than silently dropping messages. The output pump batches adjacent same-stream writes without a timer. A slow direct IOPub subscriber can slow execution.

`DapClient` is the optional language-neutral debugger transport: framed TCP, request correlation, timeouts, asynchronous events, and shutdown. Language adapters retain debugger startup, request policy, source mapping, and variable semantics.

`install_kernelspec(name, argv, display_name, language)` and `install_kernelspec_dir(path, name)` install kernelspecs without requiring jupyter_client.

## Install

```bash
pip install kernmini
```

Windows wheels are built for x64 and ARM64 with CPython 3.11–3.14. Windows kernels must set `"interrupt_mode": "message"` in their kernelspec; the Windows interrupt event used by signal-mode kernels is not supported. Protocol shutdown works, but Windows kernels do not monitor parent death or clean up child processes.
