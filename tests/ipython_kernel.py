"The real ipymini language adapter hosted by kernmini's public runner."

import asyncio, sys

from ipymini.shell import MiniShell
from kernmini import run_kernel

user_ns, first = {}, True
def shell_factory():
    global first
    shell = MiniShell(request_input=lambda *_: "", user_ns=user_ns, use_singleton=first)
    first = False
    return shell


if __name__ == "__main__": run_kernel(sys.argv[-1], shell_factory, loop_factory=asyncio.new_event_loop)
