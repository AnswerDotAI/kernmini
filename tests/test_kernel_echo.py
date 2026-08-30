"The Python adapter running a trivial shell over the native kernmini engine."

import asyncio, os, sys
from pathlib import Path

import pytest
from conkernelclient import JmsgQueues, run_kernel
from jupywire.ops import parent_id


ROOT = Path(__file__).parents[1]
ECHO_ARGV = [sys.executable, str(ROOT/'tests'/'echo_kernel.py'), "{connection_file}"]


async def _run(kc, code, **kw): return [m async for m in kc.run(code, timeout=30, **kw)]
def _one(msgs, msg_type): return next(m for m in msgs if m['msg_type'] == msg_type)
def _pubs(msgs): return [m for m in msgs if m['channel'] == 'iopub']


async def _until_stream(run, text):
    out = []
    while True:
        msg = await anext(run)
        out.append(msg)
        if msg['msg_type'] == 'stream' and msg['content']['text'] == text: return out


async def _busy(qs, msg_id):
    return await qs.jmsg_for('status', pred=lambda m: parent_id(m) == msg_id and m['content']['execution_state'] == 'busy', queue='iopub', timeout=10)


def _watch(waiter, order):
    task = asyncio.ensure_future(waiter)
    def _done(task):
        if not task.cancelled() and task.exception() is None: order.append(parent_id(task.result()))
    task.add_done_callback(_done)
    return task


async def echo_kernel_story(kc, supported_features=None, binary=False):
    info = await kc.cmd.kernel_info(timeout=30)
    content = info['content']
    assert info['msg_type'] == 'kernel_info_reply'
    assert content['implementation'] == 'echokernel' and content['language_info']['name'] == 'echo'
    assert content['supported_features'] == (supported_features or []) and content['debugger'] is False

    msgs = await _run(kc, 'hello world')
    reply = _one(msgs, 'execute_reply')
    pubs = _pubs(msgs)
    assert reply['content']['status'] == 'ok' and reply['content']['execution_count'] == 1
    assert [m['msg_type'] for m in pubs] == ['status', 'execute_input', 'stream', 'execute_result', 'status']
    assert pubs[2]['content']['text'] == 'echo: hello world\n'
    assert pubs[3]['content']['data']['text/plain'] == 'HELLO WORLD'

    msgs = await _run(kc, 'boom')
    assert _one(msgs, 'execute_reply')['content']['status'] == 'error'
    assert _one(msgs, 'error')['content']['ename'] == 'EchoError'

    if binary:
        msgs = await _run(kc, 'bytes')
        assert _one(msgs, 'execute_reply')['content']['status'] == 'ok'
        assert _one(msgs, 'execute_result')['content']['data']['image/png'] == 'cmF3'


async def execution_queue_story(kc):
    qs = JmsgQueues(kc)

    # Once the sleeper has emitted output it owns the execution lane; priority then overtakes normal.
    sleeper_id = kc.new_msg_id()
    sleeper = kc.run('sleep:0.2', msg_id=sleeper_id, timeout=5)
    await _until_stream(sleeper, 'echo: sleep:0.2\n')
    order = []
    normal_id, priority_id = kc.new_msg_id(), kc.new_msg_id()
    normal = _watch(kc.reply('normal', msg_id=normal_id, timeout=5), order)
    priority = _watch(kc.reply('priority', msg_id=priority_id, metadata=dict(priority=1), timeout=5), order)
    async for _ in sleeper: pass
    await asyncio.gather(normal, priority)
    assert order == [priority_id, normal_id]

    # A hold parks normal work, lets priority through, and completes on release.
    order = []
    held_id, normal_id, priority_id = (kc.new_msg_id() for _ in range(3))
    held = _watch(kc.reply('', msg_id=held_id, metadata=dict(hold=True), timeout=5), order)
    await _busy(qs, held_id)
    normal = _watch(kc.reply('normal', msg_id=normal_id, timeout=5), order)
    priority = _watch(kc.reply('priority', msg_id=priority_id, metadata=dict(priority=1), timeout=5), order)
    assert (await priority)['content']['status'] == 'ok' and order == [priority_id]
    assert (await kc.ctl.release(msg_id=held_id, timeout=5))['content']['found'] is True
    held_reply, normal_reply = await asyncio.gather(held, normal)
    assert [held_reply['content']['status'], normal_reply['content']['status']] == ['ok', 'ok']
    assert order == [priority_id, held_id, normal_id]
    assert (await kc.ctl.release(msg_id=held_id, timeout=5))['content']['found'] is False

    # A priority barrier proves the normal tail reached the shell queue before control releases the hold as an error.
    order = []
    held_id, normal_id, barrier_id = (kc.new_msg_id() for _ in range(3))
    held = _watch(kc.reply('', msg_id=held_id, metadata=dict(hold=True), timeout=5), order)
    await _busy(qs, held_id)
    normal = _watch(kc.reply('normal', msg_id=normal_id, timeout=5), order)
    barrier = _watch(kc.reply('barrier', msg_id=barrier_id, metadata=dict(priority=1), timeout=5), order)
    await barrier
    await kc.ctl.release(msg_id=held_id, status='error', timeout=5)
    held_reply, normal_reply = await asyncio.gather(held, normal)
    assert (held_reply['content']['status'], held_reply['content']['ename']) == ('error', 'HoldError')
    assert normal_reply['content']['status'] == 'aborted'
    assert order == [barrier_id, held_id, normal_id]

    # The same barrier makes interrupt ordering deterministic.
    order = []
    held_id, normal_id, barrier_id = (kc.new_msg_id() for _ in range(3))
    held = _watch(kc.reply('', msg_id=held_id, metadata=dict(hold=True), timeout=5), order)
    await _busy(qs, held_id)
    normal = _watch(kc.reply('normal', msg_id=normal_id, timeout=5), order)
    barrier = _watch(kc.reply('barrier', msg_id=barrier_id, metadata=dict(priority=1), timeout=5), order)
    await barrier
    await kc.interrupt(timeout=5)
    held_reply, normal_reply = await asyncio.gather(held, normal)
    assert (held_reply['content']['status'], held_reply['content']['ename']) == ('error', 'KeyboardInterrupt')
    assert normal_reply['content']['status'] == 'aborted'
    assert order == [barrier_id, held_id, normal_id]


@pytest.mark.asyncio
async def test_python_adapter_story():
    async with run_kernel('echo', ECHO_ARGV) as (_, kc):
        await echo_kernel_story(kc, ['kernel subshells'], binary=True)
        await execution_queue_story(kc)


@pytest.mark.asyncio
async def test_hold_timeout():
    env = os.environ | dict(KERNMINI_HOLD_TIMEOUT='0.2')
    async with run_kernel('echo', ECHO_ARGV, env=env) as (_, kc):
        reply = await kc.reply('', metadata=dict(hold=True), timeout=5)
        assert (reply['content']['status'], reply['content']['ename']) == ('error', 'HoldTimeout')
