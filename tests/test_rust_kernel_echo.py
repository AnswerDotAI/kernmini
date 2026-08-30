"The native Rust echo language tells its kernel protocol story through ConKernelClient."

import asyncio

import pytest
from conkernelclient import run_kernel

from test_kernel_echo import ROOT, _until_stream, _watch, echo_kernel_story


RUST_ECHO_ARGV = ['cargo', 'run', '--quiet', '--manifest-path', str(ROOT/'Cargo.toml'),
    '-p', 'kernmini', '--bin', 'kernmini-echo', '--', '{connection_file}']


@pytest.mark.asyncio
async def test_rust_language_story():
    async with run_kernel('rust-echo', RUST_ECHO_ARGV) as (_, kc):
        await echo_kernel_story(kc)

        # Synchronize on live output, then prove a failed execute aborts its queued tail.
        sleeper = kc.run('sleep:0.2', timeout=5)
        await _until_stream(sleeper, 'echo: sleep:0.2\n')
        order = []
        failed_id, aborted_id = kc.new_msg_id(), kc.new_msg_id()
        failed = _watch(kc.reply('boom', msg_id=failed_id, timeout=5), order)
        aborted = _watch(kc.reply('never', msg_id=aborted_id, timeout=5), order)
        async for _ in sleeper: pass
        failed_reply, aborted_reply = await asyncio.gather(failed, aborted)
        assert [failed_reply['content']['status'], aborted_reply['content']['status']] == ['error', 'aborted']
        assert order == [failed_id, aborted_id]
