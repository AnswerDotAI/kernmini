"""Jupyter wire-format sessions for a kernel: zmq `send`/`recv` over jupywire's wire core.

The protocol core (message construction, HMAC signing, [de]serialization; adapted from
jupyter_client, BSD-3-Clause) moved to `jupywire.session` -- one shared mirror for kernmini,
jupygate, and the kernel clients. This module keeps the zmq socket halves and the kernel
defaults: unsigned unless a key is given, username "kernel".
"""

import zmq
from jupywire.session import DELIM, Session, protocol_version


class MiniSession(Session):
    "jupywire `Session` with kernel defaults, plus zmq socket `send` and `recv`."

    def __init__(self, key: bytes = b"", signature_scheme: str = "hmac-sha256", username: str = "kernel",
        session: str | None = None, digest_history_size: int = 2**16):
        super().__init__(key=key, signature_scheme=signature_scheme, username=username, session=session,
            digest_history_size=digest_history_size)

    def send(self, stream, msg_or_type, content: dict | None = None, parent: dict | None = None,
        ident: bytes | list[bytes] | None = None, buffers: list | None = None, metadata: dict | None = None) -> dict:
        "Build (or take) a message, serialize it, and send it with optional raw buffer frames appended."
        if isinstance(msg_or_type, dict):
            msg = msg_or_type
            buffers = buffers or msg.get("buffers", [])
        else: msg = self.msg(msg_or_type, content=content, parent=parent, metadata=metadata)
        to_send = self.serialize(msg, ident)
        to_send.extend(buffers or [])
        stream.send_multipart(to_send, copy=True)
        return msg

    def recv(self, socket, mode: int = zmq.NOBLOCK, content: bool = True, copy: bool = True):
        "Receive and unpack a message; returns (idents, msg), or (None, None) when nothing is waiting."
        try: msg_list = socket.recv_multipart(mode, copy=copy)
        except zmq.ZMQError as e:
            if e.errno == zmq.EAGAIN: return None, None
            raise
        idents, msg_list = self.feed_identities(msg_list, copy)
        return idents, self.deserialize(msg_list, content=content, copy=copy)
