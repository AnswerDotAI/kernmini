import logging

from kernmini.debug import trace_msg


def test_trace_msg(caplog):
    logger = logging.getLogger("kernmini_debug.test")
    msg = dict(header=dict(msg_type="execute_request", msg_id="abc", subshell_id="s1"))
    with caplog.at_level(logging.WARNING, logger="kernmini_debug.test"):
        trace_msg(logger, "prefix", msg, enabled=True)
        mark = "done"
    assert mark == "done"
    assert "prefix" in caplog.text

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="kernmini_debug.test"):
        trace_msg(logger, "prefix", msg, enabled=False)
        mark = "done"
    assert mark == "done"
    assert caplog.text == ""
