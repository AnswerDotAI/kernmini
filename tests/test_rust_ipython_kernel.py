"The real ipymini language adapter hosted directly by kernmini and driven by ConKernelClient."

import pytest
from conkernelclient import JmsgQueues, run_kernel
from jupywire.ops import parent_id

from test_kernel_echo import ROOT, _one, _pubs, _run, _until_stream


IPYTHON_ARGV = [__import__('sys').executable, str(ROOT/'tests'/'ipython_kernel.py'), '{connection_file}']


@pytest.mark.asyncio
async def test_ipython_story():
    async with run_kernel('rust-ipython', IPYTHON_ARGV) as (_, kc):
        qs = JmsgQueues(kc)

        info = await kc.cmd.kernel_info(timeout=30)
        assert info['content']['implementation'] == 'ipymini'
        assert 'kernel subshells' in info['content']['supported_features']

        msgs = await _run(kc, "x = 41\nprint('ready')\nx + 1")
        reply, pubs = _one(msgs, 'execute_reply'), _pubs(msgs)
        assert reply['content']['status'] == 'ok'
        assert _one(msgs, 'execute_input')['content']['execution_count'] == 1
        assert [(m['content']['name'], m['content']['text']) for m in pubs if m['msg_type'] == 'stream'] == [('stdout', 'ready\n')]
        assert _one(msgs, 'execute_result')['content']['data']['text/plain'] == '42'

        child = 'sidecar'
        msgs = await _run(kc, 'x + 1', subshell_id=child)
        reply, result = _one(msgs, 'execute_reply'), _one(msgs, 'execute_result')
        assert reply['content']['status'] == 'ok' and reply['content']['execution_count'] == 1
        assert result['content']['data']['text/plain'] == '42' and result['parent_header']['subshell_id'] == child
        assert (await kc.ctl.list_subshell(timeout=5))['content']['subshell_id'] == [child]
        created = await kc.ctl.create_subshell(subshell_id=child, timeout=5)
        again = await kc.ctl.create_subshell(subshell_id=child, timeout=5)
        assert created['content'] == again['content'] == dict(status='ok', subshell_id=child)
        assert (await kc.ctl.delete_subshell(subshell_id=child, timeout=5))['content']['status'] == 'ok'
        assert (await kc.ctl.list_subshell(timeout=5))['content']['subshell_id'] == []

        caller = kc.run("import asyncio\nfrom ipymini import sidecar\nloop = asyncio.get_running_loop()\ngate2 = asyncio.Event()\nwith sidecar():\n"
            "    print('sidecar ready', flush=True)\n    await asyncio.wait_for(gate2.wait(), 5)", timeout=10)
        caller_msgs = await _until_stream(caller, 'sidecar ready\n')
        routed = await kc.reply('loop.call_soon_threadsafe(gate2.set)', timeout=5)
        caller_msgs += [m async for m in caller]
        assert routed['content']['status'] == _one(caller_msgs, 'execute_reply')['content']['status'] == 'ok'
        assert routed['content']['execution_count'] == 1
        assert (await kc.ctl.list_subshell(timeout=5))['content']['subshell_id'] == ['sidecar']

        msgs = await _run(kc, "print(input('Name: '))", on_stdin=lambda _: 'Ada')
        assert _one(msgs, 'execute_reply')['content']['status'] == 'ok'
        assert any(m['msg_type'] == 'stream' and m['content']['text'] == 'Ada\n' for m in msgs)

        complete = await kc.cmd.complete(code='x.rea', cursor_pos=5, timeout=5)
        assert any(match.endswith('real') for match in complete['content']['matches'])
        inspect = await kc.cmd.inspect(code='x', cursor_pos=1, detail_level=0, timeout=5)
        assert inspect['content']['found'] and 'int' in inspect['content']['data']['text/plain']
        complete_code = await kc.shell_request('is_complete_request', code='for i in range(2):', timeout=5)
        assert complete_code['content'] == {'status': 'incomplete', 'indent': '    '}
        history = await kc.cmd.history(hist_access_type='tail', output=False, raw=True, n=1, timeout=5)
        assert history['content']['history'][-1][-1] == "print(input('Name: '))"

        task_id = kc.new_msg_id()
        created = await _run(kc, "import asyncio\nbackground_gate = asyncio.Event()\nasync def later():\n    await background_gate.wait()\n"
            "    print('background')\n    return x + 1\ntask = asyncio.create_task(later())", msg_id=task_id)
        assert not any(m['msg_type'] == 'stream' and m['content']['text'] == 'background\n' for m in created)
        awaited = await _run(kc, 'background_gate.set()\nawait task')
        assert _one(awaited, 'execute_reply')['content']['status'] == 'ok'
        assert _one(awaited, 'execute_result')['content']['data']['text/plain'] == '42'
        background = await qs.jmsg_for('stream', pred=lambda m: parent_id(m) == task_id, queue='iopub', timeout=5)
        assert background['content']['text'] == 'background\n'

        sleeper = kc.run("print('sleeping', flush=True)\nawait asyncio.sleep(.3)", timeout=5)
        sleeper_msgs = await _until_stream(sleeper, 'sleeping\n')
        complete = await kc.cmd.complete(code='x.rea', cursor_pos=5, timeout=5)
        sleeper_msgs += [m async for m in sleeper]
        assert complete['content']['status'] == 'ok'
        assert _one(sleeper_msgs, 'execute_reply')['content']['status'] == 'ok'

        for marker, code in [('waiting', 'await asyncio.sleep(60)'), ('spinning', 'while True: pass')]:
            running = kc.run(f"print('{marker}', flush=True)\n{code}", timeout=10)
            running_msgs = await _until_stream(running, f'{marker}\n')
            assert (await kc.interrupt(timeout=5))['content']['status'] == 'ok'
            running_msgs += [m async for m in running]
            interrupted = _one(running_msgs, 'execute_reply')['content']
            assert (interrupted['status'], interrupted['ename']) == ('error', 'KeyboardInterrupt')

        missing_id = kc.new_msg_id()
        missing = await kc.shell_request('execute_request', msg_id=missing_id, timeout=5)
        states = []
        while states[-1:] != ['idle']:
            status = await qs.jmsg_for('status', pred=lambda m: parent_id(m) == missing_id, queue='iopub', timeout=5)
            states.append(status['content']['execution_state'])
        assert missing['content']['status'] == 'error' and missing['content']['ename'] == 'MissingField'
        assert states == ['busy', 'idle']
        missing = await kc.shell_request('complete_request', timeout=5)
        assert missing['content']['status'] == 'error' and missing['content']['matches'] == []
