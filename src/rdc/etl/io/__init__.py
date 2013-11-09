# -*- coding: utf-8 -*-
#
# Copyright 2012-2013 Romain Dorgueil
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
from rdc.etl.hash import Hash
from Queue import Queue as BaseQueue

STDIN = 0
STDIN2 = 1
STDOUT = 0
STDERR = 1

class Token(object):
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return '<%s>' % (self.name, )

EndOfStream = Token('EndOfStream')

BUFFER_SIZE = 8192

class TerminatedIOError(IOError):
    pass


class TerminatedInputError(TerminatedIOError):
    pass


class TerminatedOutputError(TerminatedIOError):
    pass


class IQueue(object):
    @abstract
    def put(self, data, block=True, timeout=None, channel=None):
        pass

    @abstract
    def get(self, block=True, timeout=None, channel=None):
        pass


class Queue(IQueue, BaseQueue):
    def __init__(self, maxsize=BUFFER_SIZE):
        BaseQueue.__init__(self, maxsize)
        self.empty()
        self._eos_received = False
        self.terminated = False

    def put(self, item, block=True, timeout=None):
        if self._eos_received:
            raise TerminatedOutputError('Cannot put() on a queue (%r) that already has received the EndOfStream marker.' % (self, ))

        if item == EndOfStream:
            self._eos_received = True

        return BaseQueue.put(self, item, block, timeout)

    def get(self, block=True, timeout=None):
        if self.terminated:
            raise TerminatedInputError('No more data will come from this queue. Ever.')

        v = BaseQueue.get(self, block, timeout)

        if v == EndOfStream:
            assert self._eos_received, 'Integrity problem: a queue should not be able to terminate without receiving the EndOfStream signal first.'
            self.terminated = True
            raise TerminatedInputError('Queue just got termination signal.')

        return v


class SingleItemQueue(Queue):
    def __init__(self, maxsize=BUFFER_SIZE):
        Queue.__init__(self, maxsize)
        self.put(Hash())
        self.put(EndOfStream)

class SinkQueue(Queue):
    def put(self, item, block=True, timeout=None):
        """This queue is a /dev/null"""
        pass



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

    def __init__(self, maxsize=BUFFER_SIZE, tails=None):
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


class QueueCollection(IQueue):
    def __init__(self, channels):
        self.queues = dict([(channel, None) for channel in channels])

    def get_queue(self, channel=0):
        if not channel in self.queues:
            raise KeyError('Cannot get queue for non existant channel %r.' % (channel))

        return self.queues[channel]

    def set_queue(self, queue, channel=0):
        if not channel in self.queues:
            raise KeyError('Cannot set queue for non existant channel %r.' % (channel))

        if self.queues[channel] is None:
            self.queues[channel] = queue
        else:
            raise NotImplementedError('multi input queues not implemented')

    def get_queues(self):
        return [self.get_queue(channel) for channel in self.queues]

    def put(self, data, block=True, timeout=None, channel=None):
        self.get_queue(channel or 0).put(data, block=block, timeout=timeout)

    def get(self, block=True, timeout=None, channel=None):
        return self.get_queue(channel or 0).get(block=block, timeout=timeout)

    @property
    def plugged(self):
        if not len(self.queues):
            # todo is this right ? can we consider that a no queue channel collection is "plugged" ?
            return True

        for id, queue in self.queues.items():
            if queue is not None:
                return True

        return False

    @property
    def unplugged_channels(self):
        return [channel for channel in self.queues if self.queues[channel] is None]

class OutputQueueCollection(QueueCollection):
    def __init__(self, channels=(STDOUT, STDERR, )):
        super(OutputQueueCollection, self).__init__(channels)

    def get(self, block=True, timeout=None, channel=None):
        raise IOError('This channel is write-only, as a well disciplined output.')

    def put_all(self, data):
        for channel in self.queues:
            self.put(data, channel=channel)


class InputQueueCollection(QueueCollection):
    def __init__(self, channels=(STDIN, )):
        super(InputQueueCollection, self).__init__(channels)

    def put(self, data, block=True, timeout=None, channel=None):
        raise IOError('This channel is read-only, as a well disciplined input.')

    def plug(self, channel_collection, channel=0, channel_from=0):
        q = channel_collection.get_queue(channel_from)
        assert q, 'Source queue must be initialized.'
        self.set_queue(q, channel)

    def get_any(self):
        # todo documentation says .enpty() value is not reliable ... XXX
        while not self.terminated:
            for id, queue in self.queues.items():
                if not queue.empty():
                    return queue.get(), id
            time.sleep(0.2)

        raise TerminatedInputError('This input collection is terminated.')

    @property
    def terminated(self):
        for id, queue in self.queues.items():
            if queue and not queue.terminated:
                return False
            return True


QUEUE_COLLECTIONS = {
    'input': InputQueueCollection,
    'output': OutputQueueCollection,
}

