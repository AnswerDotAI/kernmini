"The real ipymini language adapter hosted directly by kernmini."

import asyncio, sys

from ipymini.shell import MiniShell
from kernmini._native import run_kernel


async def main():
    user_ns, first = {}, True
    def shell_factory():
        nonlocal first
        shell = MiniShell(request_input=lambda *_: "", user_ns=user_ns, use_singleton=first)
        first = False
        return shell
    await run_kernel(sys.argv[-1], shell_factory, asyncio.new_event_loop)


if __name__ == "__main__":
    with asyncio.Runner() as runner: runner.run(main())
