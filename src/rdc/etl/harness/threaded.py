# -*- coding: utf-8 -*-
#
# Author: Romain Dorgueil <romain@dorgueil.net>
# Copyright: © 2011-2013 SARL Romain Dorgueil Conseil
#
from threading import Thread, RLock, Semaphore
import types
import time
from rdc.etl.harness.base import Harness
from rdc.etl.hash import Hash
from rdc.etl.stream import Stream
from rdc.etl.transform.extract import Extract
import sys

EOQ = object()

class TransformThread(Thread):
    def __init__(self, worker_pool, transform):
        super(TransformThread, self).__init__()

        self.worker_pool = worker_pool
        self.transform = transform

        self.output_stream = Stream()

    def plug_from(self, brick):
        self.input_stream = brick.output_stream.create_pipe()

    def run(self):
        transform = self.transform
        output_stream = self.output_stream

        semaphore = Semaphore(0)
        wu_count = 0

        while self.input_stream.alive:
            if not len(self.input_stream):
                time.sleep(1)
                continue

            input = []
            for i in range(0, 4):
                if not len(self.input_stream): break
                el = self.input_stream.read()
                if not el == Stream.EOS:
                    input.append(el)
            if not len(input):
                continue

            def _work_unit(input=input, semaphore=semaphore):
                for _in in input:
                    _out = transform(_in)
                    if isinstance(_out, types.GeneratorType):
                        for _ in _out:
                            output_stream.write(_)
                    elif _out is not None:
                        output_stream.write(_out)
                semaphore.release()

            self.worker_pool.add_work_unit(_work_unit)
            wu_count += 1

        for i in range(0, wu_count):
            semaphore.acquire()

        # problem : this is sent before the work units are executed.
        output_stream.end()

    def __repr__(self):
        return (self.is_alive() and '+' or '-') + ' ' + repr(self.transform)

class WorkerThread(Thread):
    def __init__(self):
        super(WorkerThread, self).__init__()

        self.work_queue = []
        self.terminate = False

    def run(self):
        while True:
            if len(self.work_queue):
                _callable, _args, _kwargs = self.work_queue.pop()
                _callable(*_args, **_kwargs)
            else:
                if self.terminate:
                    return
                time.sleep(1)

class ThreadedHarness(object):
    def __init__(self, worker_count=4):
        self.worker_count = worker_count
        self.workers = []
        self.transforms = []

    def start(self):
        for i in range(0, self.worker_count):
            worker = WorkerThread()
            self.workers.append(worker)
            worker.start()
        for transform in self.transforms:
            transform.start()

    def join(self):
        time.sleep(0.1)
        while True:
            is_alive = False
            for transform in self.transforms:
                is_this_alive = transform.is_alive()
                print "\033[K", transform
                is_alive = is_alive or is_this_alive
            if not is_alive:
                break
            sys.stdout.write("\033[F"*(len(self.transforms)))
            time.sleep(0.25)
        for transform in self.transforms:
            transform.join()
        self.terminate()
        print 'All done.'

    def add_work_units(self, work):
        while len(work):
            max_work_per_worker = max(1, min(len(work) / len(self.workers), 16))
            for worker in self.workers:
                _cnt = max(0, max_work_per_worker - len(worker.work_queue))
                if _cnt:
                    worker.work_queue += work[0:_cnt]
                    work = work[_cnt:]
                    if not len(work): break
            if not len(work): break

    def add_work_unit(self, callable, *args, **kwargs):
        self.add_work_units([(callable, args, kwargs, )])

    def add(self, transform):
        t = TransformThread(self, transform)
        self.transforms.append(t)
        return t

    def terminate(self):
        for worker in self.workers:
            worker.terminate = True
        for worker in self.workers:
            worker.join()
