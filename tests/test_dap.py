"The DAP client driving a real debugpy session."

import queue, socket, subprocess, sys, time

import pytest

from kernmini._native import DapClient


def _port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _event(events, kind, timeout=10):
    deadline, seen = time.monotonic() + timeout, []
    while time.monotonic() < deadline:
        try: event = events.get(timeout=deadline - time.monotonic())
        except queue.Empty: break
        seen.append(event.get("event"))
        if event.get("event") == kind: return event
    raise TimeoutError(f"no {kind!r} DAP event; got {seen}")


def _request(client, command, **arguments):
    response = client.send_request(dict(type="request", command=command, arguments=arguments))
    assert response["success"], response
    return response


@pytest.fixture
def debugpy_client():
    port, events = _port(), queue.Queue()
    adapter = subprocess.Popen([sys.executable, "-m", "debugpy.adapter", "--host", "127.0.0.1", "--port", str(port)])
    client = DapClient(events.put)
    deadline = time.monotonic() + 10
    while True:
        try:
            client.connect("127.0.0.1", port)
            break
        except RuntimeError:
            if adapter.poll() is not None: raise RuntimeError(f"debugpy adapter exited with {adapter.returncode}")
            if time.monotonic() >= deadline: raise TimeoutError("debugpy adapter did not start")
            time.sleep(.02)
    try: yield client, events
    finally:
        client.close()
        if adapter.poll() is None: adapter.terminate()
        adapter.wait(timeout=5)


def test_debugpy_session(debugpy_client, tmp_path):
    client, events = debugpy_client
    program = tmp_path / "debuggee.py"
    program.write_text("x = 41\ny = x + 1\nprint(y)\n")

    initialized = _request(client, "initialize", clientID="kernmini-test", adapterID="python", pathFormat="path",
        linesStartAt1=True, columnsStartAt1=True, supportsRunInTerminalRequest=False)
    assert initialized["body"]["supportsConfigurationDoneRequest"]

    launch_seq, launch = client.send_request_async(dict(type="request", command="launch",
        arguments=dict(program=str(program), cwd=str(tmp_path), console="internalConsole", justMyCode=False)))
    _event(events, "initialized")
    breakpoints = _request(client, "setBreakpoints", source=dict(path=str(program)), breakpoints=[dict(line=2)])
    assert breakpoints["body"]["breakpoints"][0]["verified"]
    _request(client, "configurationDone")
    assert client.wait_for_response(launch_seq, launch)["success"]

    stopped = _event(events, "stopped")
    thread_id = stopped["body"]["threadId"]
    stack = _request(client, "stackTrace", threadId=thread_id)
    frame = stack["body"]["stackFrames"][0]
    assert frame["source"]["path"] == str(program)
    scopes = _request(client, "scopes", frameId=frame["id"])
    locals_ref = next(scope["variablesReference"] for scope in scopes["body"]["scopes"] if scope["name"] == "Locals")
    variables = _request(client, "variables", variablesReference=locals_ref)
    assert any(variable["name"] == "x" and variable["value"] == "41" for variable in variables["body"]["variables"])

    _request(client, "continue", threadId=thread_id)
    _event(events, "terminated")
