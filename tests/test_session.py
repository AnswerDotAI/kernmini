"MiniSession wire-format tests: round-trips, signing guards, and cross-verification with jupyter_client where installed."

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


def test_parent_and_buffers():
    s = MiniSession(key=b"k")
    parent = s.msg("a_request", {})
    reply = s.msg("a_reply", dict(status="ok"), parent=parent)
    frames = s.serialize(reply) + [b"rawbuf"]
    _, rest = s.feed_identities(frames)
    out = s.deserialize(rest)
    assert out["parent_header"]["msg_id"] == parent["header"]["msg_id"]
    assert bytes(out["buffers"][0]) == b"rawbuf"


def test_replay_tamper_unsigned():
    s = MiniSession(key=b"secret")
    _, rest = _wire(s, s.msg("a_request", dict(x=1)))
    s.deserialize(list(rest))
    with pytest.raises(ValueError, match="Duplicate Signature"): s.deserialize(list(rest))
    bad = list(rest)
    bad[0], bad[4] = b"0" * len(bad[0]), b'{"x":2}'
    with pytest.raises(ValueError, match="Invalid Signature"): s.deserialize(bad)
    with pytest.raises(ValueError, match="Unsigned Message"): s.deserialize([b""] + list(rest)[1:])


def test_no_auth():
    s = MiniSession()
    _, rest = _wire(s, s.msg("b_request", {}))
    assert s.deserialize(rest)["msg_type"] == "b_request"


def test_cross_verify_jupyter_client():
    jcs = pytest.importorskip("jupyter_client.session")
    key = b"shared"
    ms, js = MiniSession(key=key), jcs.Session(key=key)
    idents, rest = js.feed_identities(ms.serialize(ms.msg("execute_request", dict(code="6*7")), ident=[b"c"]))
    out = js.deserialize(rest)
    assert idents == [b"c"] and out["content"]["code"] == "6*7"
    idents2, rest2 = ms.feed_identities(js.serialize(js.msg("kernel_info_reply", dict(status="ok")), ident=b"c2"))
    out2 = ms.deserialize(rest2)
    assert idents2 == [b"c2"] and out2["content"]["status"] == "ok"
