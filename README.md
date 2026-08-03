# kernmini

Everything a Jupyter kernel needs except the language.

kernmini is the language-agnostic core of a Jupyter kernel: connection files, HMAC-signed wire sessions, the full socket thread cast (shell/control async routers, IOPub with a bounded queue and JEP 65 welcomes, stdin routing, heartbeat), busy/idle and abort discipline, stop-on-error, interrupts, JEP 91 subshells, kernelspec installation, and process lifecycle (signal handling, process-group isolation and teardown, parent-watch crash safety). A kernel author supplies only an executor (the *shell*) and gets a correct, tested kernel around it.

[ipymini](https://github.com/AnswerDotAI/ipymini) is the reference kernel (IPython). Its integration suite is kernmini's main proving ground.

## A complete kernel

```python
from contextlib import contextmanager
from kernmini import run_kernel

class EchoShell:
    def __init__(self, request_input=None, **kw): self.execution_count,self._stream = 0,None

    def set_stream_sender(self, sender): self._stream = sender

    @contextmanager
    def execution_context(self, allow_stdin, silent): yield

    def kernel_info(self):
        return dict(implementation="echokernel", implementation_version="0.0.1", banner="echo",
            language_info=dict(name="echo", version="1.0", mimetype="text/plain", file_extension=".txt"))

    async def execute(self, code, silent=False, store_history=True, user_expressions=None, allow_stdin=False):
        self.execution_count += 1
        if self._stream: self._stream("stdout", f"echo: {code}\n")
        return dict(execution_count=self.execution_count, result={"text/plain": code.upper()})

run_kernel(connection_file, EchoShell, subshells=False)
```

That's the whole kernel. Every Jupyter client (JupyterLab, nbclient, jupygate, ...) can now launch it, execute against it, stream its output, and interrupt and shut it down. `kernmini.install_kernelspec(name, argv, display_name, language)` registers it with Jupyter.

The shell contract is documented in `DEV.md`. Beyond the four members above, everything is opt-in by capability: implement `complete`/`inspect`/`is_complete`/`history` for language services, `debug_request` for a DAP debugger, `interrupt` for language-specific sync interruption, `bind_kernel` for a backref, and pass `subshells=True` when the shell supports concurrent instances.

## Install

```bash
pip install kernmini
```
