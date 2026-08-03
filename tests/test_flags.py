from kernmini.debug import DebugFlags, envbool


def test_debug_flags(monkeypatch):
    for value in ["", "0", "false", "no"]:
        monkeypatch.setenv("KERNMINI_FLAG", value)
        assert envbool("KERNMINI_FLAG") is False
    for value in ["1", "true", "yes", "on"]:
        monkeypatch.setenv("KERNMINI_FLAG", value)
        assert envbool("KERNMINI_FLAG") is True

    monkeypatch.setenv("TEST_DEBUG", "1")
    monkeypatch.setenv("TEST_DEBUG_MSGS", "true")
    flags = DebugFlags.from_env("TEST")
    assert flags.enabled is True
    assert flags.trace_msgs is True
