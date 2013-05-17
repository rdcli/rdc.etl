# -*- coding: utf-8 -*-
#
# Author: Romain Dorgueil <romain@dorgueil.net>
# Copyright: © 2011-2013 SARL Romain Dorgueil Conseil
#
from Queue import Queue
from threading import Thread
import types
import time
from rdc.etl.hash import Hash
import sys

EOQ = object()

class SingleItemQueue(Queue):
    def __init__(self, maxsize=0):
        Queue.__init__(self, maxsize)
        self.put(Hash())
        self.put(EOQ)

class MultiTailQueue(Queue):
    def __init__(self, maxsize=0):
        Queue.__init__(self, maxsize)

        self._tails = []

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
        self.output = MultiTailQueue()

    def set_input_from(self, io_transform):
        if isinstance(io_transform.output, MultiTailQueue):
            self.input = io_transform.output.create_tail()
        else:
            self.input = io_transform.output

    def run(self):
        input = self.input or SingleItemQueue()

        while True:
            _in = input.get()

            if _in == EOQ:
                self.output.put(EOQ)
                break

            _out = self.transform(_in)

            if isinstance(_out, types.GeneratorType):
                for item in _out:
                    self.output.put(item)
            elif _out is not None:
                self.output.put(_out)

    def __repr__(self):
        return (self.is_alive() and '+' or '-') + ' ' + repr(self.transform)


class ThreadedHarness(object):
    def __init__(self):
        self.transforms = []

    def start(self):
        for transform in self.transforms:
            transform.start()

    def join(self):
        while True:
            is_alive = False
            for transform in self.transforms:
                is_alive = is_alive or transform.is_alive()
            if not is_alive:
                break
            time.sleep(0.25)
        for transform in self.transforms:
            transform.join()

    def add(self, transform):
        t = ThreadedTransform(transform)
        self.transforms.append(t)
        return t

