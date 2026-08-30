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
        self.execution_count = 0
        self.stream = None

    def set_stream_sender(self, sender): self.stream = sender

    def kernel_info(self):
        return dict(implementation="echo", implementation_version="0.1", banner="echo",
            language_info=dict(name="echo", version="0.1", mimetype="text/plain", file_extension=".txt"))

    async def execute(self, code, **kwargs):
        self.execution_count += 1
        if self.stream: self.stream("stdout", f"echo: {code}\n")
        return dict(execution_count=self.execution_count, result={"text/plain": code.upper()})


run_kernel(sys.argv[-1], EchoShell, own_process_group=True)
```

`run_kernel` creates a persistent asyncio event loop and runs the Rust engine until shutdown. It uses loopmini when installed and the standard asyncio loop otherwise; `loop_factory=` can select one explicitly. The factory is also used to create independent language sessions for JEP 91 subshells. Standalone executables can request process-group ownership, while embedded kernels leave their host process group unchanged by default.

Rust language implementations use the `Language` and `LanguageSession` traits directly. `ExecutionContext` provides stream, display, stdin, interrupt, and subshell routing without exposing Jupyter transport details.

`DapClient` is the optional language-neutral debugger transport: framed TCP, request correlation, timeouts, asynchronous events, and shutdown. Language adapters retain debugger startup, request policy, source mapping, and variable semantics.

`install_kernelspec(name, argv, display_name, language)` and `install_kernelspec_dir(path, name)` install kernelspecs without requiring jupyter_client.

## Install

```bash
pip install kernmini
```
