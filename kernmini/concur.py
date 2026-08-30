"In-cell routing to temporary and persistent subshells."

import contextvars
from contextlib import contextmanager

_subshell = contextvars.ContextVar("kernmini_subshell", default=None)


@contextmanager
def subshell():
    "Run execute_requests arriving from this cell's client session in a fresh subshell while the body runs."
    sub = _subshell.get()
    if sub is None: raise RuntimeError("subshell() only works inside a cell running under a kernmini kernel")
    sid = sub.open_subshell()
    try: yield sid
    finally: sub.close_subshell(sid, delete=True)


@contextmanager
def sidecar():
    "Route execute requests from this cell's client session through the persistent sidecar."
    sub = _subshell.get()
    if sub is None: raise RuntimeError("sidecar() only works inside a cell running under a kernmini kernel")
    sid = sub.open_subshell("sidecar")
    try: yield sid
    finally: sub.close_subshell(sid, delete=False)
