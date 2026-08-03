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
