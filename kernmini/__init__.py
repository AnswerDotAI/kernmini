"Everything a Jupyter kernel needs except the language."

import asyncio

from .concur import unlock, subshell
from .kernelspec import install_kernelspec, install_kernelspec_dir


def run_kernel(connection_file, shell_factory, *, own_process_group=False):
    "Run a Python shell factory as a Jupyter kernel."
    from ._native import new_event_loop, run_kernel as run_native
    async def run(): await run_native(connection_file, shell_factory, own_process_group=own_process_group)
    with asyncio.Runner(loop_factory=new_event_loop) as runner: return runner.run(run())


def __getattr__(name):
    if name == "__version__":
        from ._native import __version__
        return __version__
    raise AttributeError(name)
