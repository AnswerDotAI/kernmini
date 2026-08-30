"The minimal Python language adapter used by kernmini's client-driven tests."

import sys
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
        if code.startswith("sleep:"):
            import asyncio
            await asyncio.sleep(float(code[6:]))
        if code == "boom": return dict(execution_count=self.execution_count, error=dict(ename="EchoError", evalue=code, traceback=[]))
        if code == "bytes": return dict(execution_count=self.execution_count, result={"image/png": b"raw"})
        return dict(execution_count=self.execution_count, result={"text/plain": code.upper()})


if __name__ == "__main__": run_kernel(sys.argv[-1], EchoShell)
