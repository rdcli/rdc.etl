# -*- coding: utf-8 -*-
#
# Author: Romain Dorgueil <romain@dorgueil.net>
# Copyright: © 2011-2013 SARL Romain Dorgueil Conseil
#
import time, types
from Queue import Queue as BaseQueue
from threading import Thread
import traceback
from rdc.etl.hash import Hash
from rdc.etl.harness import AbstractHarness

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
    """
    A multi-tail queue is a regular one input channel queue that sends everything it gets in into its "tail" queues. If
    possible, it will "copy" the input item, so we avoid concurrency transform problems. As every input data is getting
    instantaneously flushed to the tails, get() method does not have any sense, and thus this should not be used as a
    transform input, instead, use a tail for this.

    The aim of this class is to be able to plug one output to multiple transformations' inputs, without having to worry
    about concurrency. We may be a bit concerned about memory usage later (TODO), but for any reasonable use, it should
    be ok.

    """
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
            # No output yet ? Let's create a basic queue.
            self.input = Queue()
            io_transform.output = self.input
        elif isinstance(io_transform.output, Queue):
            # Already a simple queue there ? Let's make it multi-tailed.
            q = io_transform.output
            io_transform.output = MultiTailQueue(tails=[q])
            self.input = io_transform.output.create_tail()
        elif isinstance(io_transform.output, MultiTailQueue):
            # More than one output already, just need a new tail.
            self.input = io_transform.output.create_tail()
        else:
            raise TypeError('I dont know what kind of output this is, man ...')

        return io_transform

    def _add_output(self, value):
        if value:
            if isinstance(value, types.GeneratorType):
                for item in value:
                    if self.output is not None:
                        self.output.put(item)
            else:
                if self.output is not None:
                    self.output.put(value)

    def run(self):
        input = self.input or SingleItemQueue()

        self._add_output(self.transform.initialize())

        while True:
            _in = input.get()

            if _in == EOQ:
                self._add_output(self.transform.finalize())
                if self.output is not None:
                    self.output.put(EOQ)
                break

            try:
                _out = self.transform(_in)
                self._add_output(_out)
            except Exception, e:
                print 'Exception caught in transform():', e.__class__.__name__, e.args[0]
                traceback.print_exc()
                break

    def __repr__(self):
        return (self.is_alive() and '+' or '-') + ' ' + repr(self.transform)


class ThreadedHarness(AbstractHarness):
    def loop(self):
        for transform in self._transforms:
            transform.start()

        # Alive loop
        while True:
            is_alive = False
            for transform in self._transforms:
                is_alive = is_alive or transform.is_alive()
            self.update_status()
            if not is_alive:
                break
            time.sleep(0.2)

        # Wait for all transform threads to finish
        for transform in self._transforms:
            transform.join()

    def update_status(self):
        for status in self.status:
            status.update(self._transforms)

    # Methods below does not belong to API.
    def __init__(self):
        super(ThreadedHarness, self).__init__()
        self._transforms = []

        # pointer to last added transform, wo we can use the chain_add shortcut
        self._last_transform = None

    def add(self, transform):
        t = ThreadedTransform(transform)
        self._transforms.append(t)
        return t

    def chain_add(self, *transforms):
        for transform in transforms:
            io_transform = self.add(transform)
            if self._last_transform:
                io_transform.set_input_from(self._last_transform)
            self._last_transform = io_transform


