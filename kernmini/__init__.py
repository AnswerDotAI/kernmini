"""Everything a Jupyter kernel needs except the language.

kernmini is the language-agnostic core of a Jupyter kernel: connection files, HMAC-signed wire
sessions, the socket thread cast (shell/control routers, IOPub, stdin, heartbeat), busy/idle and
abort discipline, interrupts, subshells (JEP 91), and process lifecycle. A kernel supplies a
*shell*: an execution layer built by the `shell_factory` passed to `MiniKernel`/`run_kernel`.
See DEV.md for the shell contract; `ipymini` is the reference implementation (IPython), and a
minimal shell needs only `execute`, `execution_count`, `execution_context`, and
`set_stream_sender`.
"""
__version__ = "0.1.1"



from .kernel import MiniKernel, run_kernel
from .session import MiniSession
from .concur import unlock, subshell
from .kernelspec import install_kernelspec, install_kernelspec_dir
