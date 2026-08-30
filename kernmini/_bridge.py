import asyncio, contextvars
from contextlib import nullcontext
from .concur import _subshell, sidecar, subshell

_current = contextvars.ContextVar("kernmini.execution", default=None)


class _IOPub:
    def send(self, msg_type, parent=None, content=None, metadata=None, ident=None, buffers=None, **kwargs):
        sink = _current.get()
        if sink is not None: sink.publish(msg_type, content or kwargs, metadata or {}, ident, buffers or [])


class NativeKernel:
    def __init__(self, target): self.target,self.iopub = target,_IOPub()
    def subshell(self): return subshell()
    def sidecar(self): return sidecar()
    def current_parent(self):
        sink = _current.get()
        return sink.parent() if sink is not None else {}
    def get_parent(self, channel=None): return self.current_parent()
    @property
    def comm_manager(self): return self.target.comm_manager


def kernel_proxy(target): return NativeKernel(target)


async def execute(target, current, sink, code, **kwargs):
    "Run one Python execution with its task-local routing and capture context."
    token = current.set(sink)
    subshell_token = _subshell.set(sink)
    sink.started(asyncio.current_task())
    try:
        context = target.execution_context(allow_stdin=kwargs["allow_stdin"], silent=kwargs["silent"]) \
            if hasattr(target, "execution_context") else nullcontext()
        with context: return await target.execute(code, **kwargs)
    finally:
        _subshell.reset(subshell_token)
        current.reset(token)


def request(target, method, content):
    if method == "comm_info" and not hasattr(target, method): return {"status": "ok", "comms": {}}
    return getattr(target, method)(**content)


async def request_async(target, method, content): return request(target, method, content)


async def message(target, current, sink, msg_type, content, buffers):
    token = current.set(sink)
    try:
        context = target.output_context() if hasattr(target, "output_context") else nullcontext()
        if hasattr(target, "message"):
            with context: target.message(msg_type, content, list(buffers))
    finally: current.reset(token)
