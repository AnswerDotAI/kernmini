"Debug infrastructure for kernels: env flags, logging/faulthandler setup, message tracing, and debugger cell-filename hashing."

import faulthandler, logging, os, signal, sys, tempfile
from dataclasses import dataclass


def envbool(name: str) -> bool:
    v = (os.environ.get(name) or "").strip().lower()
    return v not in ("", "0", "false", "no")


@dataclass(frozen=True)
class DebugFlags:
    enabled: bool = False
    trace_msgs: bool = False

    @classmethod
    def from_env(cls, prefix: str = "KERNMINI") -> "DebugFlags": return cls(enabled=envbool(f"{prefix}_DEBUG"), trace_msgs=envbool(f"{prefix}_DEBUG_MSGS"))


def setup_debug(flags: DebugFlags):
    "Initialize debug infrastructure: logging, faulthandler, SIGUSR1 handler."
    if not flags.enabled: return
    root = logging.getLogger()
    fmt = "%(asctime)s %(levelname)s %(name)s: %(message)s"
    if not root.handlers: logging.basicConfig(level=logging.DEBUG, stream=sys.__stderr__, format=fmt)
    faulthandler.enable(file=sys.__stderr__)
    if hasattr(signal, "SIGUSR1"): faulthandler.register(signal.SIGUSR1, file=sys.__stderr__)


def trace_msg(logger, prefix: str, msg: dict, *, enabled: bool = True):
    "Log message flow at high level: msg_type, msg_id, subshell_id."
    if not enabled: return
    h = msg.get("header") or {}
    logger.warning("%s type=%s id=%s subshell=%r", prefix, h.get("msg_type"), h.get("msg_id"), h.get("subshell_id"))


def murmur2_x86(data: str, seed: int) -> int:
    "Return Murmur2 x86 hash of UTF-8 `data` with `seed`."
    m = 0x5BD1E995
    data_bytes = data.encode("utf-8")
    length = len(data_bytes)
    h = seed ^ length
    rounded_end = length & 0xFFFFFFFC
    for i in range(0, rounded_end, 4):
        k = int.from_bytes(data_bytes[i : i + 4], "little")
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> 24
        k = (k * m) & 0xFFFFFFFF

        h = (h * m) & 0xFFFFFFFF
        h ^= k

    val = length & 0x03
    k = 0
    if val >= 3: k = data_bytes[rounded_end + 2] << 16
    if val >= 2: k |= data_bytes[rounded_end + 1] << 8
    if val >= 1:
        k |= data_bytes[rounded_end]
        h ^= k
        h = (h * m) & 0xFFFFFFFF

    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= h >> 15
    return h


DEBUG_HASH_SEED = 0xC70F6907


def debug_tmp_directory() -> str: return os.path.join(tempfile.gettempdir(), f"kernmini_{os.getpid()}")


def debug_cell_filename(code: str, ext: str = ".py") -> str:
    "Compute debug cell filename (hash matches ipykernel's scheme, so frontends map breakpoints); respects KERNMINI_CELL_NAME."
    cell_name = os.environ.get("KERNMINI_CELL_NAME")
    if cell_name is None:
        name = murmur2_x86(code, DEBUG_HASH_SEED)
        cell_name = os.path.join(debug_tmp_directory(), f"{name}{ext}")
    return cell_name
