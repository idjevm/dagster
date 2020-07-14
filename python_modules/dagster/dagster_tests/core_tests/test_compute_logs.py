from __future__ import print_function

import os
import signal
import time

from dagster import seven
from dagster.core.execution.compute_logs import (
    mirror_stream_to_file,
    redirect_to_file,
    tail_to_stream,
)
from dagster.utils import ensure_file, get_multiprocessing_context, pid_exists


def test_redirect_file():
    with seven.TemporaryDirectory() as tempdir:
        target_path = os.path.join(tempdir, 'target.out')
        redirected_path = os.path.join(tempdir, 'redirected.out')
        with open(target_path, 'a+') as target:
            with redirect_to_file(target, redirected_path):
                print('This is some file redirect mumbo jumbo', file=target, end='')

        with open(target_path, 'r') as f:
            assert not f.read()

        with open(redirected_path, 'r') as f:
            assert f.read() == 'This is some file redirect mumbo jumbo'


def test_mirror_stream_to_file():
    with seven.TemporaryDirectory() as tempdir:
        target_path = os.path.join(tempdir, 'target.out')
        redirected_path = os.path.join(tempdir, 'redirected.out')
        with open(target_path, 'a+') as target:
            with mirror_stream_to_file(target, redirected_path):
                print('This is some mirror stream mumbo jumbo', file=target, end='')
                time.sleep(1)

        with open(target_path, 'r') as f:
            assert f.read() == 'This is some mirror stream mumbo jumbo'

        with open(redirected_path, 'r') as f:
            assert f.read() == 'This is some mirror stream mumbo jumbo'


def test_tail_to_stream():
    with seven.TemporaryDirectory() as tempdir:
        target_path = os.path.join(tempdir, 'target.out')
        tailed_path = os.path.join(tempdir, 'tailed.out')

        with open(target_path, 'a+') as target:
            ensure_file(tailed_path)
            with tail_to_stream(tailed_path, target):
                with open(tailed_path, 'a+') as tailed:
                    print('This is some tailed mumbo jumbo', file=tailed, end='')
                time.sleep(1)

        with open(target_path, 'r') as f:
            assert f.read() == 'This is some tailed mumbo jumbo'

        with open(tailed_path, 'r') as f:
            assert f.read() == 'This is some tailed mumbo jumbo'


def spawn_infinite_tail_process(queue, target_path, out_path):
    with open(target_path, 'a+') as target_stream:
        with open(out_path, 'a+') as out_stream:
            with tail_to_stream(target_path, out_stream) as pid_info:

                # somehow communicate back the pid_info
                queue.put(pid_info)

                while True:
                    print('hello', file=target_stream)
                    time.sleep(0.1)


def test_orphans():
    with seven.TemporaryDirectory() as tempdir:
        target_path = os.path.join(tempdir, 'target.out')
        out_path = os.path.join(tempdir, 'outfile.out')

        multiprocessing_context = get_multiprocessing_context()
        queue = multiprocessing_context.Queue()
        process = multiprocessing_context.Process(
            target=spawn_infinite_tail_process, args=(queue, target_path, out_path)
        )
        process.start()

        info = None
        while not info:
            info = queue.get(block=True, timeout=5)

        try:
            tail_pid, watcher_pid = info
            assert tail_pid

            process.terminate()

            if watcher_pid:
                time.sleep(0.3)
                assert not pid_exists(watcher_pid)
        finally:
            try:
                os.kill(tail_pid, signal.SIGTERM)
            except OSError:
                pass
            try:
                os.kill(watcher_pid, signal.SIGTERM)
            except OSError:
                pass
