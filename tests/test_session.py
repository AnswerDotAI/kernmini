"MiniSession subclass checks: signed round-trip and the unsigned kernel default (the wire core is tested in jupywire)."

import pytest

from kernmini.session import MiniSession


def _wire(sess, msg, ident=None):
    frames = sess.serialize(msg, ident=ident)
    idents, rest = sess.feed_identities(frames)
    return idents, list(rest)


def test_roundtrip_signed():
    s = MiniSession(key=b"secret")
    idents, rest = _wire(s, s.msg("execute_request", dict(code="1+1")), ident=[b"c1"])
    out = s.deserialize(rest)
    assert idents == [b"c1"]
    assert out["msg_type"] == "execute_request" and out["content"]["code"] == "1+1"
    assert out["parent_header"] == {} and out["buffers"] == []



def test_no_auth():
    s = MiniSession()
    _, rest = _wire(s, s.msg("b_request", {}))
    assert s.deserialize(rest)["msg_type"] == "b_request"


