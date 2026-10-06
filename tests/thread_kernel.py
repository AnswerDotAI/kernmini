"A synchronous interpreter using ThreadWorker and native interruption."

import asyncio, json, sys, threading
from pathlib import Path

from kernmini import ThreadWorker, run_kernel
from echo_kernel import EchoShell


class Interpreter:
    def __init__(self, stream, stopped, record):
        self.stream,self.stopped,self.record = stream,stopped,record
        record['init'] = threading.get_ident()

    def execute(self, code):
        self.record['execute'] = threading.get_ident()
        self.stream('stdout', f'worker: {code}\n')
        if code == 'wait':
            assert self.stopped.wait(10), 'interrupt hook was not called'
            self.stopped.clear()
            return dict(error=dict(ename='NativeInterrupt', evalue='', traceback=[]))
        return dict(result={'text/plain': str(threading.get_ident())})

    def __del__(self): self.record['drop'] = threading.get_ident()


class ThreadShell(EchoShell):
    def __init__(self):
        super().__init__()
        self.loop = asyncio.get_running_loop()
        self.stopped,self.record = threading.Event(),dict(loop=threading.get_ident())

    def bind_kernel(self, kernel): assert asyncio.get_event_loop() is self.loop

    @classmethod
    async def create(cls):
        shell = cls()
        shell.worker = await ThreadWorker.start(
            lambda: Interpreter(lambda *args: shell._stream(*args), shell.stopped, shell.record), stack_size=64*1024*1024)
        return shell

    async def execute(self, code, **kwargs):
        return await self.worker.call(lambda interpreter: interpreter.execute(code))

    async def is_complete(self, code): return await self.worker.call(lambda interpreter: dict(status='complete'))

    def interrupt(self):
        self.record['interrupt'] = threading.get_ident()
        self.stopped.set()

    async def shutdown(self):
        self.record['shutdown'] = threading.get_ident()
        await self.worker.shutdown()
        Path(sys.argv[1]).write_text(json.dumps(self.record))


if __name__ == '__main__': run_kernel(sys.argv[-1], ThreadShell.create)
