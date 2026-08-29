"The native Rust echo language tells the complete kernel protocol story."

import json, subprocess

import pytest, zmq

from client import MiniSession
from test_kernel_echo import _await_welcome, _drain_iopub, _ports, _replies, _request, _send, _sock, echo_kernel_story


@pytest.fixture
def rust_echo_kernel(tmp_path):
    key = "test-key-123"
    shell_p, iopub_p, stdin_p, control_p, hb_p = _ports(5)
    conn = dict(transport="tcp", ip="127.0.0.1", shell_port=shell_p, iopub_port=iopub_p, stdin_port=stdin_p,
        control_port=control_p, hb_port=hb_p, key=key, signature_scheme="hmac-sha256")
    cf = tmp_path / "conn.json"
    cf.write_text(json.dumps(conn))
    root = __import__("pathlib").Path(__file__).parents[1]
    proc = subprocess.Popen(["cargo", "run", "--quiet", "--manifest-path", str(root / "Cargo.toml"),
        "-p", "kernmini", "--bin", "kernmini-echo", "--", str(cf)])
    ctx = zmq.Context.instance()
    sess = MiniSession(key=key.encode(), username="testclient")
    _drain_iopub.sess = MiniSession(key=key.encode())
    shell, control, sub = _sock(ctx, zmq.DEALER, shell_p), _sock(ctx, zmq.DEALER, control_p), _sock(ctx, zmq.SUB, iopub_p)
    _await_welcome(sub)
    try: yield proc, sess, shell, control, sub
    finally:
        for socket in (shell, control, sub): socket.close(0)
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)


def test_rust_echo_kernel_end_to_end(rust_echo_kernel): echo_kernel_story(rust_echo_kernel)


def test_rust_execution_queue(rust_echo_kernel):
    _, sess, shell, control, sub = rust_echo_kernel
    _request(shell, sess, "kernel_info_request", {}, timeout=30)
    _drain_iopub(sub)

    sleeper = _send(shell, sess, "execute_request", dict(code="sleep:0.2"))
    __import__("time").sleep(.05)
    normal = _send(shell, sess, "execute_request", dict(code="normal"))
    priority = _send(shell, sess, "execute_request", dict(code="priority"), metadata=dict(priority=1))
    assert [mid for mid, _ in _replies(shell, sess, 3)] == [sleeper, priority, normal]

    held = _send(shell, sess, "execute_request", dict(code=""), metadata=dict(hold=True))
    __import__("time").sleep(.05)
    normal = _send(shell, sess, "execute_request", dict(code="normal"))
    priority = _send(shell, sess, "execute_request", dict(code="priority"), metadata=dict(priority=1))
    (priority_reply, _), = _replies(shell, sess, 1)
    assert priority_reply == priority
    release = _request(control, sess, "release_request", dict(msg_id=held))
    assert release["content"]["found"] is True
    assert [mid for mid, _ in _replies(shell, sess, 2)] == [held, normal]
    assert _request(control, sess, "release_request", dict(msg_id=held))["content"]["found"] is False

    held = _send(shell, sess, "execute_request", dict(code=""), metadata=dict(hold=True))
    __import__("time").sleep(.05)
    normal = _send(shell, sess, "execute_request", dict(code="normal"))
    _request(control, sess, "release_request", dict(msg_id=held, status="error"))
    replies = _replies(shell, sess, 2)
    assert [mid for mid, _ in replies] == [held, normal]
    assert [content["status"] for _, content in replies] == ["error", "aborted"]

    held = _send(shell, sess, "execute_request", dict(code=""), metadata=dict(hold=True))
    __import__("time").sleep(.05)
    normal = _send(shell, sess, "execute_request", dict(code="normal"))
    _request(control, sess, "interrupt_request", {})
    replies = _replies(shell, sess, 2)
    assert [mid for mid, _ in replies] == [held, normal]
    assert [(content["status"], content.get("ename")) for _, content in replies] == [("error", "KeyboardInterrupt"), ("aborted", None)]

    sleeper = _send(shell, sess, "execute_request", dict(code="sleep:0.2"))
    __import__("time").sleep(.05)
    failed = _send(shell, sess, "execute_request", dict(code="boom"))
    aborted = _send(shell, sess, "execute_request", dict(code="never"))
    replies = _replies(shell, sess, 3)
    assert [mid for mid, _ in replies] == [sleeper, failed, aborted]
    assert [content["status"] for _, content in replies] == ["ok", "error", "aborted"]
