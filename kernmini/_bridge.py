import asyncio, contextvars, inspect
from contextlib import contextmanager, nullcontext
from .concur import _subshell, sidecar, subshell

_current = contextvars.ContextVar("kernmini.execution", default=None)


def run_loop(loop, fut=None):
    "Run `loop` until `fut` is done (forever if None), re-entering after a `SystemExit` escapes a task, which asyncio raises out of the loop"
    while True:
        try: return loop.run_until_complete(fut) if fut is not None else loop.run_forever()
        except SystemExit:
            if fut is not None and fut.done(): raise


@contextmanager
def session_loop(loop_factory):
    with asyncio.Runner(loop_factory=loop_factory) as runner:
        loop = runner.get_loop()
        asyncio.set_event_loop(loop)
        try: yield loop
        finally: asyncio.set_event_loop(None)


def run_child_loop(factory, loop_factory, ready):
    with session_loop(loop_factory) as loop:
        target = run_loop(loop, loop.create_task(create_shell(factory)))
        ready(loop, target)
        run_loop(loop)


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


def stream(name, text):
    sink = _current.get()
    if sink is not None: sink.stream(name, text)


def display(event):
    sink = _current.get()
    if sink is not None: sink.display(event)


def request_input(prompt, password):
    sink = _current.get()
    if sink is not None: return sink.input(prompt, password)
    from ._native import KernelError
    error = KernelError('input requested outside an execution')
    error.kind = 'Unavailable'
    raise error


def bind_shell(target):
    for method,callback in [('set_stream_sender', stream), ('set_display_sender', display), ('set_input_sender', request_input)]:
        if hasattr(target, method): getattr(target, method)(callback)
    if hasattr(target, 'bind_kernel'): target.bind_kernel(NativeKernel(target))


async def create_shell(factory):
    target = factory()
    return await target if inspect.isawaitable(target) else target


async def execute(target, sink, code, **kwargs):
    "Run one Python execution with its task-local routing and capture context."
    token = _current.set(sink)
    subshell_token = _subshell.set(sink)
    sink.started(asyncio.current_task())
    try:
        context = target.execution_context(allow_stdin=kwargs["allow_stdin"], silent=kwargs["silent"]) \
            if hasattr(target, "execution_context") else nullcontext()
        with context: return await target.execute(code, **kwargs)
    finally:
        _subshell.reset(subshell_token)
        _current.reset(token)


async def request(target, method, content):
    if not hasattr(target, method): return
    result = getattr(target, method)(**content)
    return await result if inspect.isawaitable(result) else result


async def shutdown(target):
    if hasattr(target, "shutdown"):
        result = target.shutdown()
        if inspect.isawaitable(result): await result


async def message(target, sink, msg_type, content, buffers):
    token = _current.set(sink)
    try:
        context = target.output_context() if hasattr(target, "output_context") else nullcontext()
        if hasattr(target, "message"):
            with context:
                result = target.message(msg_type, content, buffers)
                if inspect.isawaitable(result): await result
    finally: _current.reset(token)
