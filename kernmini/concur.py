"In-cell opt-ins for concurrent execution: unlock() and subshell()."

import contextvars
from contextlib import contextmanager

_release = contextvars.ContextVar("kernmini_release", default=None)
_subshell = contextvars.ContextVar("kernmini_subshell", default=None)


def unlock()->bool:
    "Let queued shell messages run while the current cell awaits; irreversible for the rest of the cell."
    release = _release.get()
    if release is None: return False
    release()
    return True


@contextmanager
def subshell():
    "Run execute_requests arriving from this cell's client session in a fresh subshell while the body runs."
    sub = _subshell.get()
    if sub is None: raise RuntimeError("subshell() only works inside a cell running under a kernmini kernel")
    sid = sub.open_subshell()
    try: yield sid
    finally: sub.close_subshell(sid)
