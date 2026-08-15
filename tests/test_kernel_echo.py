"End-to-end proof that kernmini runs a kernel with no IPython: a trivial echo shell, driven over real sockets by MiniSession."

import json, socket, subprocess, sys, time

import pytest, zmq

from kernmini.session import MiniSession

RUNNER = '''
import sys
from contextlib import contextmanager
from kernmini import run_kernel


class EchoShell:
    "The minimal shell contract: execute, execution_count, execution_context, set_stream_sender."

    def __init__(self, request_input=None, **kw):
        self.execution_count = 0
        self._stream = None

    def set_stream_sender(self, sender): self._stream = sender

    @contextmanager
    def execution_context(self, allow_stdin, silent): yield

    def kernel_info(self):
        return dict(implementation="echokernel", implementation_version="0.0.1", banner="echo",
            language_info=dict(name="echo", version="1.0", mimetype="text/plain", file_extension=".txt"))

    async def execute(self, code, silent=False, store_history=True, user_expressions=None, allow_stdin=False):
        self.execution_count += 1
        if self._stream: self._stream("stdout", f"echo: {code}\\n")
        if code.startswith("sleep:"):
            import asyncio
            await asyncio.sleep(float(code[6:]))
        if code == "boom": return dict(execution_count=self.execution_count, error=dict(ename="EchoError", evalue=code, traceback=[]))
        return dict(execution_count=self.execution_count, result={"text/plain": code.upper()})


run_kernel(sys.argv[-1], EchoShell, subshells=False)
'''


def _sock(ctx, typ, port):
    s = ctx.socket(typ)
    s.linger = 0
    if typ == zmq.SUB: s.setsockopt(zmq.SUBSCRIBE, b"")
    s.connect(f"tcp://127.0.0.1:{port}")
    return s


def _ports(n):
    socks = [socket.socket() for _ in range(n)]
    for s in socks: s.bind(("127.0.0.1", 0))
    ports = [s.getsockname()[1] for s in socks]
    for s in socks: s.close()
    return ports


def _drain_iopub(sub, until_idle=True, timeout=10.0):
    "Collect iopub msg dicts until an idle status (skipping the welcome)."
    sess, out = _drain_iopub.sess, []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not sub.poll(200): continue
        frames = sub.recv_multipart()
        idents, rest = sess.feed_identities(frames)
        msg = sess.deserialize(rest)
        if msg["msg_type"] == "iopub_welcome": continue
        out.append(msg)
        if until_idle and msg["msg_type"] == "status" and msg["content"]["execution_state"] == "idle": return out
    raise TimeoutError(f"no idle within {timeout}s; got {[m['msg_type'] for m in out]}")


def _await_welcome(sub, timeout=60.0):
    "Wait for the JEP 65 iopub_welcome: proof the subscription is live, so no later message can be missed."
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not sub.poll(200): continue
        _, rest = _drain_iopub.sess.feed_identities(sub.recv_multipart())
        if _drain_iopub.sess.deserialize(rest)["msg_type"] == "iopub_welcome": return
    raise TimeoutError("no iopub_welcome")


def _request(sock, sess, msg_type, content, timeout=10.0):
    sock.send_multipart(sess.serialize(sess.msg(msg_type, content)))
    if not sock.poll(timeout * 1000): raise TimeoutError(f"no reply to {msg_type}")
    idents, rest = sess.feed_identities(sock.recv_multipart())
    return sess.deserialize(rest)


@pytest.fixture
def echo_kernel(tmp_path):
    key = "test-key-123"
    shell_p, iopub_p, stdin_p, control_p, hb_p = _ports(5)
    conn = dict(transport="tcp", ip="127.0.0.1", shell_port=shell_p, iopub_port=iopub_p, stdin_port=stdin_p,
        control_port=control_p, hb_port=hb_p, key=key, signature_scheme="hmac-sha256")
    cf = tmp_path / "conn.json"
    cf.write_text(json.dumps(conn))
    runner = tmp_path / "echo_runner.py"
    runner.write_text(RUNNER)
    proc = subprocess.Popen([sys.executable, str(runner), str(cf)], stderr=subprocess.PIPE)
    ctx = zmq.Context.instance()
    sess = MiniSession(key=key.encode(), username="testclient")
    _drain_iopub.sess = MiniSession(key=key.encode())
    shell, control, sub = _sock(ctx, zmq.DEALER, shell_p), _sock(ctx, zmq.DEALER, control_p), _sock(ctx, zmq.SUB, iopub_p)
    _await_welcome(sub)
    try: yield proc, sess, shell, control, sub
    finally:
        for s in (shell, control, sub): s.close(0)
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)


def test_echo_kernel_end_to_end(echo_kernel):
    proc, sess, shell, control, sub = echo_kernel

    info = _request(shell, sess, "kernel_info_request", {}, timeout=30)
    assert info["msg_type"] == "kernel_info_reply"
    c = info["content"]
    assert c["implementation"] == "echokernel" and c["language_info"]["name"] == "echo"
    assert c["supported_features"] == [] and c["debugger"] is False

    _drain_iopub(sub)  # busy/idle for kernel_info
    reply = _request(shell, sess, "execute_request", dict(code="hello world"))
    msgs = _drain_iopub(sub)
    assert reply["content"]["status"] == "ok" and reply["content"]["execution_count"] == 1
    kinds = [m["msg_type"] for m in msgs]
    assert kinds == ["status", "execute_input", "stream", "execute_result", "status"]
    assert msgs[2]["content"]["text"] == "echo: hello world\n"
    assert msgs[3]["content"]["data"]["text/plain"] == "HELLO WORLD"

    reply = _request(shell, sess, "execute_request", dict(code="boom"))
    msgs = _drain_iopub(sub)
    assert reply["content"]["status"] == "error" and reply["content"]["ename"] == "EchoError"
    assert any(m["msg_type"] == "error" for m in msgs)

    reply = _request(control, sess, "shutdown_request", dict(restart=False))
    assert reply["content"]["status"] == "ok"
    assert proc.wait(timeout=10) in (0, -9)  # group-leader kernels SIGKILL their own process group as the designed last act


def _send(sock, sess, msg_type, content, metadata=None):
    "Send without awaiting the reply; returns the msg_id."
    m = sess.msg(msg_type, content, metadata=metadata)
    sock.send_multipart(sess.serialize(m))
    return m["header"]["msg_id"]


def _replies(sock, sess, n, timeout=10.0):
    "Collect `n` shell replies in arrival order as (parent_msg_id, content) pairs."
    out, deadline = [], time.monotonic() + timeout
    while len(out) < n and time.monotonic() < deadline:
        if not sock.poll(200): continue
        _, rest = sess.feed_identities(sock.recv_multipart())
        msg = sess.deserialize(rest)
        out.append((msg["parent_header"]["msg_id"], msg["content"]))
    assert len(out) == n, f"expected {n} replies, got {len(out)}"
    return out


def test_priority_and_hold(echo_kernel):
    proc, sess, shell, control, sub = echo_kernel
    _request(shell, sess, "kernel_info_request", {}, timeout=30)
    _drain_iopub(sub)

    # priority: a queued higher-priority execute overtakes a queued normal one
    mid_s = _send(shell, sess, "execute_request", dict(code="sleep:0.3"))
    time.sleep(0.1)  # the sleeper takes the baton; the next two queue behind it
    mid_a = _send(shell, sess, "execute_request", dict(code="a"))
    mid_b = _send(shell, sess, "execute_request", dict(code="b"), metadata=dict(priority=1))
    order = [mid for mid, c in _replies(shell, sess, 3)]
    assert order == [mid_s, mid_b, mid_a], "priority 1 must overtake the queued normal execute"

    # hold: parks the queue; higher priority passes, normal waits, release completes
    mid_h = _send(shell, sess, "execute_request", dict(code=""), metadata=dict(hold=True))
    time.sleep(0.1)
    mid_x = _send(shell, sess, "execute_request", dict(code="x"))
    mid_y = _send(shell, sess, "execute_request", dict(code="y"), metadata=dict(priority=1))
    (got_y, c_y), = _replies(shell, sess, 1)
    assert got_y == mid_y and c_y["status"] == "ok", "priority 1 must run during the hold"
    rel = _request(control, sess, "release_request", dict(msg_id=mid_h))
    assert rel["content"]["status"] == "ok" and rel["content"]["found"] is True
    (got_h, c_h), (got_x, c_x) = _replies(shell, sess, 2)
    assert (got_h, c_h["status"]) == (mid_h, "ok"), "release completes the hold"
    assert (got_x, c_x["status"]) == (mid_x, "ok"), "the parked normal execute runs after release"
    rel = _request(control, sess, "release_request", dict(msg_id=mid_h))
    assert rel["content"]["found"] is False, "a completed hold is gone; late release is a quiet no-op"

    # release with status=error: the hold errors and aborts the queued tail
    mid_h2 = _send(shell, sess, "execute_request", dict(code=""), metadata=dict(hold=True))
    time.sleep(0.1)
    mid_z = _send(shell, sess, "execute_request", dict(code="z"))
    time.sleep(0.1)  # let z reach the shell queue: control and shell are separate sockets with no cross-channel ordering
    _request(control, sess, "release_request", dict(msg_id=mid_h2, status="error"))
    (got_h2, c_h2), (got_z, c_z) = _replies(shell, sess, 2)
    assert (got_h2, c_h2["status"], c_h2["ename"]) == (mid_h2, "error", "HoldError")
    assert (got_z, c_z["status"]) == (mid_z, "aborted"), "an error hold aborts the queued tail"

    # interrupt during a hold: the hold aborts, and so does the queued tail
    mid_h3 = _send(shell, sess, "execute_request", dict(code=""), metadata=dict(hold=True))
    time.sleep(0.1)
    mid_w = _send(shell, sess, "execute_request", dict(code="w"))
    time.sleep(0.1)  # as above: w must be queued before the interrupt lands
    _request(control, sess, "interrupt_request", {})
    (got_h3, c_h3), (got_w, c_w) = _replies(shell, sess, 2)
    assert (got_h3, c_h3["status"], c_h3["ename"]) == (mid_h3, "error", "KeyboardInterrupt")
    assert (got_w, c_w["status"]) == (mid_w, "aborted")


def test_hold_timeout(tmp_path):
    key = "test-key-123"
    shell_p, iopub_p, stdin_p, control_p, hb_p = _ports(5)
    conn = dict(transport="tcp", ip="127.0.0.1", shell_port=shell_p, iopub_port=iopub_p, stdin_port=stdin_p,
        control_port=control_p, hb_port=hb_p, key=key, signature_scheme="hmac-sha256")
    cf = tmp_path / "conn.json"
    cf.write_text(json.dumps(conn))
    runner = tmp_path / "echo_runner.py"
    runner.write_text(RUNNER)
    import os
    env = os.environ | dict(KERNMINI_HOLD_TIMEOUT="0.2")
    proc = subprocess.Popen([sys.executable, str(runner), str(cf)], stderr=subprocess.PIPE, env=env)
    ctx = zmq.Context.instance()
    sess = MiniSession(key=key.encode(), username="testclient")
    _drain_iopub.sess = MiniSession(key=key.encode())
    shell, control, sub = _sock(ctx, zmq.DEALER, shell_p), _sock(ctx, zmq.DEALER, control_p), _sock(ctx, zmq.SUB, iopub_p)
    try:
        _await_welcome(sub)
        mid_h = _send(shell, sess, "execute_request", dict(code=""), metadata=dict(hold=True))
        (got_h, c_h), = _replies(shell, sess, 1, timeout=5)
        assert (got_h, c_h["status"], c_h["ename"]) == (mid_h, "error", "HoldTimeout")
    finally:
        for s in (shell, control, sub): s.close(0)
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)
