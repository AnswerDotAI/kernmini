"Everything a Jupyter kernel needs except the language."

import asyncio

from ._bridge import run_loop
from .concur import sidecar, subshell


def _default_loop_factory():
    try: from loopmini import new_event_loop
    except ImportError: return asyncio.new_event_loop
    return new_event_loop


def run_kernel(connection_file, shell_factory, *, loop_factory=None, own_process_group=False):
    "Run a Python shell factory as a Jupyter kernel."
    from ._native import run_kernel as run_native
    if loop_factory is None: loop_factory = _default_loop_factory()
    async def run(): await run_native(connection_file, shell_factory, loop_factory, own_process_group=own_process_group)
    with asyncio.Runner(loop_factory=loop_factory) as runner:
        loop = runner.get_loop()
        return run_loop(loop, loop.create_task(run()))


def __getattr__(name):
    if name in ("__version__", "install_kernelspec", "install_kernelspec_dir"):
        from . import _native
        return getattr(_native, name)
    raise AttributeError(name)
