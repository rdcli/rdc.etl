# -*- coding: utf-8 -*-
#
# Author: Romain Dorgueil <romain@dorgueil.net>
# Copyright: © 2011-2013 SARL Romain Dorgueil Conseil
#
from Queue import Queue as BaseQueue
from threading import Thread
import types
import time
from rdc.etl.hash import Hash

EOQ = object()
QUEUE_MAX_SIZE = 8192

class Queue(BaseQueue):
    def __init__(self, maxsize=QUEUE_MAX_SIZE):
        BaseQueue.__init__(self, maxsize)

class SingleItemQueue(Queue):
    def __init__(self, maxsize=QUEUE_MAX_SIZE):
        Queue.__init__(self, maxsize)
        self.put(Hash())
        self.put(EOQ)

class MultiTailQueue(Queue):
    def __init__(self, maxsize=QUEUE_MAX_SIZE, tails=None):
        Queue.__init__(self, maxsize)

        self._tails = tails or []

    def put(self, item, block=True, timeout=None):
        for tail in self._tails:
            tail.put(hasattr(item, 'copy') and item.copy() or item, block, timeout)

    def get(self, block=True, timeout=None):
        raise RuntimeError('You cannot get() on a multi tail queue.')

    def create_tail(self):
        tail = Queue()
        self._tails.append(tail)
        return tail

class ThreadedTransform(Thread):
    def __init__(self, transform):
        Thread.__init__(self)

        self.transform = transform

        self.input = None
        self.output = None

    def set_input_from(self, io_transform):
        if io_transform.output is None:
            self.input = Queue()
            io_transform.output = self.input
        elif isinstance(io_transform.output, Queue):
            q = io_transform.output
            io_transform.output = MultiTailQueue(tails=[q])
            self.input = io_transform.output.create_tail()
        elif isinstance(io_transform.output, MultiTailQueue):
            self.input = io_transform.output.create_tail()
        else:
            raise TypeError('I dont know what kind of output this is, man ...')

    def run(self):
        input = self.input or SingleItemQueue()

        while True:
            _in = input.get()

            if _in == EOQ:
                if self.output is not None:
                    self.output.put(EOQ)
                break

            _out = self.transform(_in)

            if isinstance(_out, types.GeneratorType):
                for item in _out:
                    if self.output is not None:
                        self.output.put(item)
            elif _out is not None:
                if self.output is not None:
                    self.output.put(_out)

            # del _out ?

    def __repr__(self):
        return (self.is_alive() and '+' or '-') + ' ' + repr(self.transform)


class ThreadedHarness(object):
    def __init__(self):
        self._transforms = []

    def start(self):
        for transform in self._transforms:
            transform.start()

    def join(self):
        while True:
            is_alive = False
            for transform in self._transforms:
                is_alive = is_alive or transform.is_alive()
            self.update_status()
            if not is_alive:
                break
            time.sleep(0.2)
        for transform in self._transforms:
            transform.join()

    def add(self, transform):
        t = ThreadedTransform(transform)
        self._transforms.append(t)
        return t

    def update_status(self):
        pass


