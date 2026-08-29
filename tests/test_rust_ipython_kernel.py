"The Rust kernel engine hosting ipymini's real MiniShell."

import json, subprocess, sys, time

import pytest, zmq

from client import MiniSession
from test_kernel_echo import _await_welcome, _drain_iopub, _ports, _replies, _request, _send, _sock


RUNNER = '''
import asyncio, sys
from ipymini.shell import MiniShell
from kernmini._native import new_event_loop, run_kernel


async def main():
    user_ns, first = {}, True
    def shell_factory():
        nonlocal first
        shell = MiniShell(request_input=lambda *_: "", user_ns=user_ns, use_singleton=first)
        first = False
        return shell
    await run_kernel(sys.argv[-1], shell_factory)


with asyncio.Runner(loop_factory=new_event_loop) as runner: runner.run(main())
'''


@pytest.fixture
def rust_ipython_kernel(tmp_path):
    key = "test-key-123"
    shell_p, iopub_p, stdin_p, control_p, hb_p = _ports(5)
    conn = dict(transport="tcp", ip="127.0.0.1", shell_port=shell_p, iopub_port=iopub_p, stdin_port=stdin_p,
        control_port=control_p, hb_port=hb_p, key=key, signature_scheme="hmac-sha256")
    cf = tmp_path / "conn.json"
    cf.write_text(json.dumps(conn))
    runner = tmp_path / "runner.py"
    runner.write_text(RUNNER)
    proc = subprocess.Popen([sys.executable, str(runner), str(cf)])
    ctx = zmq.Context.instance()
    sess = MiniSession(key=key.encode(), username="testclient")
    _drain_iopub.sess = MiniSession(key=key.encode())
    identity = b"testclient"
    shell = _sock(ctx, zmq.DEALER, shell_p, identity)
    control = _sock(ctx, zmq.DEALER, control_p, identity)
    stdin = _sock(ctx, zmq.DEALER, stdin_p, identity)
    sub = _sock(ctx, zmq.SUB, iopub_p)
    try:
        _await_welcome(sub)
        yield proc, sess, shell, control, stdin, sub
    finally:
        for socket in (shell, control, stdin, sub): socket.close(0)
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)


def _wait_stream(sub, text, timeout=10):
    deadline, seen = time.monotonic() + timeout, []
    while time.monotonic() < deadline:
        if not sub.poll(200): continue
        _, rest = _drain_iopub.sess.feed_identities(sub.recv_multipart())
        msg = _drain_iopub.sess.deserialize(rest)
        seen.append((msg["msg_type"], msg["content"]))
        if msg["msg_type"] == "stream" and text in msg["content"]["text"]: return msg
    raise TimeoutError(f"no stream containing {text!r}; got {seen}")


def test_ipython_story(rust_ipython_kernel):
    proc, sess, shell, control, stdin, sub = rust_ipython_kernel

    info = _request(shell, sess, "kernel_info_request", {}, timeout=30)
    assert info["content"]["implementation"] == "ipymini"
    assert "kernel subshells" in info["content"]["supported_features"]
    _drain_iopub(sub)

    reply = _request(shell, sess, "execute_request", dict(code="x = 41\nprint('ready')\nx + 1"))
    msgs = _drain_iopub(sub)
    assert reply["content"]["status"] == "ok"
    execute_input, = (m for m in msgs if m["msg_type"] == "execute_input")
    assert execute_input["content"]["execution_count"] == 1
    assert [(m["content"]["name"], m["content"]["text"]) for m in msgs if m["msg_type"] == "stream"] == [("stdout", "ready\n")]
    result, = (m for m in msgs if m["msg_type"] == "execute_result")
    assert result["content"]["data"]["text/plain"] == "42"

    created = _request(control, sess, "create_subshell_request", {}, timeout=30)
    child = created["content"]["subshell_id"]
    assert _request(control, sess, "list_subshell_request", {})["content"]["subshell_id"] == [child]
    child_id = _send(shell, sess, "execute_request", dict(code="x + 1"), subshell_id=child)
    (reply_id, child_reply), = _replies(shell, sess, 1)
    assert reply_id == child_id and child_reply["status"] == "ok" and child_reply["execution_count"] == 1
    msgs = _drain_iopub(sub)
    result, = (m for m in msgs if m["msg_type"] == "execute_result")
    assert result["content"]["data"]["text/plain"] == "42" and result["parent_header"]["subshell_id"] == child
    assert _request(control, sess, "delete_subshell_request", dict(subshell_id=child))["content"]["status"] == "ok"
    assert _request(control, sess, "list_subshell_request", {})["content"]["subshell_id"] == []

    caller = _send(shell, sess, "execute_request", dict(code="import asyncio\nfrom ipymini import subshell\nloop = asyncio.get_running_loop()\ngate2 = asyncio.Event()\nwith subshell():\n    print('subshell ready', flush=True)\n    await asyncio.wait_for(gate2.wait(), 5)"))
    _wait_stream(sub, "subshell ready")
    routed = _send(shell, sess, "execute_request", dict(code="loop.call_soon_threadsafe(gate2.set)"))
    replies = dict(_replies(shell, sess, 2))
    assert replies[caller]["status"] == replies[routed]["status"] == "ok"
    assert replies[routed]["execution_count"] == 1
    _drain_iopub(sub)
    _drain_iopub(sub)
    assert _request(control, sess, "list_subshell_request", {})["content"]["subshell_id"] == []

    input_id = _send(shell, sess, "execute_request", dict(code="print(input('Name: '))", allow_stdin=True))
    assert stdin.poll(10_000), "no input_request"
    _, stdin_request = sess.recv(stdin)
    assert stdin_request["content"] == {"prompt": "Name: ", "password": False}
    sess.send(stdin, "input_reply", {"value": "Ada"}, parent=stdin_request)
    (reply_id, input_reply), = _replies(shell, sess, 1)
    assert reply_id == input_id and input_reply["status"] == "ok"
    msgs = _drain_iopub(sub)
    assert any(m["msg_type"] == "stream" and m["content"]["text"] == "Ada\n" for m in msgs)

    complete = _request(shell, sess, "complete_request", dict(code="x.rea", cursor_pos=5))
    _drain_iopub(sub)
    assert any(match.endswith("real") for match in complete["content"]["matches"])

    inspect = _request(shell, sess, "inspect_request", dict(code="x", cursor_pos=1, detail_level=0))
    _drain_iopub(sub)
    assert inspect["content"]["found"] and "int" in inspect["content"]["data"]["text/plain"]

    complete_code = _request(shell, sess, "is_complete_request", dict(code="for i in range(2):"))
    _drain_iopub(sub)
    assert complete_code["content"] == {"status": "incomplete", "indent": "    "}

    history = _request(shell, sess, "history_request", dict(hist_access_type="tail", output=False, raw=True, n=1))
    _drain_iopub(sub)
    assert history["content"]["history"][-1][-1] == "print(input('Name: '))"

    task_id = _send(shell, sess, "execute_request", dict(code="import asyncio\nasync def later():\n    await asyncio.sleep(.01)\n    print('background')\n    return x + 1\ntask = asyncio.create_task(later())"))
    (task_reply_id, task_reply), = _replies(shell, sess, 1)
    assert task_reply_id == task_id and task_reply["status"] == "ok"
    _drain_iopub(sub)
    reply = _request(shell, sess, "execute_request", dict(code="await task"))
    msgs = _drain_iopub(sub)
    assert reply["content"]["status"] == "ok"
    result, = (m for m in msgs if m["msg_type"] == "execute_result")
    assert result["content"]["data"]["text/plain"] == "42"
    background, = (m for m in msgs if m["msg_type"] == "stream" and m["content"]["text"] == "background\n")
    assert background["parent_header"]["msg_id"] == task_id

    waiter = _send(shell, sess, "execute_request", dict(code="from ipymini import unlock\ngate = asyncio.Event()\nassert unlock()\nprint('unlocked', flush=True)\nawait asyncio.wait_for(gate.wait(), 5)"))
    _wait_stream(sub, "unlocked")
    setter = _send(shell, sess, "execute_request", dict(code="gate.set()"))
    replies = dict(_replies(shell, sess, 2))
    assert replies[waiter]["status"] == replies[setter]["status"] == "ok"
    _drain_iopub(sub)
    _drain_iopub(sub)

    sleeper = _send(shell, sess, "execute_request", dict(code="print('sleeping', flush=True)\nawait asyncio.sleep(.3)"))
    _wait_stream(sub, "sleeping")
    completer = _send(shell, sess, "complete_request", dict(code="x.rea", cursor_pos=5))
    (first_id, first), (second_id, second) = _replies(shell, sess, 2)
    assert first_id == completer and first["status"] == "ok"
    assert second_id == sleeper and second["status"] == "ok"
    _drain_iopub(sub)
    _drain_iopub(sub)

    for code in ("await asyncio.sleep(60)", "while True: pass"):
        interrupted = _send(shell, sess, "execute_request", dict(code=code))
        time.sleep(.1)
        assert _request(control, sess, "interrupt_request", {})["content"]["status"] == "ok"
        (reply_id, reply), = _replies(shell, sess, 1)
        assert reply_id == interrupted and (reply["status"], reply["ename"]) == ("error", "KeyboardInterrupt")
        _drain_iopub(sub)

    missing = _request(shell, sess, "execute_request", {})
    msgs = _drain_iopub(sub)
    assert missing["content"]["status"] == "error" and missing["content"]["ename"] == "MissingField"
    assert [m["content"]["execution_state"] for m in msgs if m["msg_type"] == "status"] == ["busy", "idle"]
    missing = _request(shell, sess, "complete_request", {})
    assert missing["content"]["status"] == "error" and missing["content"]["matches"] == []

    reply = _request(control, sess, "shutdown_request", dict(restart=False))
    assert reply["content"]["status"] == "ok"
    assert proc.wait(timeout=10) == 0
