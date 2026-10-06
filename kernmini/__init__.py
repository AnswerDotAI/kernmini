"Everything a Jupyter kernel needs except the language."

import asyncio, os

from ._bridge import create_shell, run_loop, session_loop
from .concur import sidecar, subshell


def _default_loop_factory():
    try: from loopmini import new_event_loop
    except ImportError: return asyncio.new_event_loop
    return new_event_loop


def run_kernel(connection_file, shell_factory, *, loop_factory=None, own_process_group=False):
    "Run a Python shell factory as a Jupyter kernel."
    from ._native import run_kernel as run_native
    owns_process_group,parent_pid = False,0
    if os.name == 'posix':
        parent_pid = os.getppid()
        if own_process_group and os.getpgrp() != os.getpid():
            try: os.setpgid(0, 0)
            except OSError: pass
        owns_process_group = own_process_group and os.getpgrp() == os.getpid()
    if loop_factory is None: loop_factory = _default_loop_factory()
    async def run():
        shell = await create_shell(shell_factory)
        await run_native(connection_file, shell, shell_factory, loop_factory, owns_process_group, parent_pid)
    with session_loop(loop_factory) as loop: return run_loop(loop, loop.create_task(run()))


def __getattr__(name):
    if name in ("__version__", "install_kernelspec", "install_kernelspec_dir", "KernelError", "ThreadWorker"):
        from . import _native
        return getattr(_native, name)
    raise AttributeError(name)
