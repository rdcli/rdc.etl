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

STDIN = 0
STDIN2 = 1
STDOUT = 0
STDERR = 1

from Queue import Queue as BaseQueue

EndOfStream = object()
BUFFER_SIZE = 8192


class TerminatedIOError(IOError):
    pass


class TerminatedInputError(TerminatedIOError):
    pass


class TerminatedOutputError(TerminatedIOError):
    pass


class Queue(BaseQueue):
    def __init__(self, maxsize=BUFFER_SIZE):
        BaseQueue.__init__(self, maxsize)
        self.empty()
        self._eos_received = False
        self.terminated = False

    def put(self, item, block=True, timeout=None):
        if self._eos_received:
            raise TerminatedOutputError('Cannot put() on a queue that already has received the EndOfStream marker.')

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

        return v


class SingleItemQueue(Queue):
    def __init__(self, maxsize=BUFFER_SIZE):
        Queue.__init__(self, maxsize)
        self.put(Hash())
        self.put(EndOfStream)


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


class CommunicationChannelCollection(object):
    DEFAULT_CHANNEL = None

    def __init__(self, channels):
        self.queues = {channel: None for channel in channels}

    def get_queue(self, channel=None):
        if channel is None:
            channel = self.DEFAULT_CHANNEL

        if not channel in self.queues:
            raise KeyError('Cannot get queue for non existant channel %r.' % (channel))

        if self.queues[channel] is None:
            self.queues[channel] = Queue()

        return self.queues[channel]

    def set_queue(self, queue, channel=None):
        if channel is None:
            channel = self.DEFAULT_CHANNEL

        if not channel in self.queues:
            raise KeyError('Cannot set queue for non existant channel %r.' % (channel))

        if self.queues[channel] is None:
            self.queues[channel] = queue
        else:
            raise NotImplementedError('multi input queues not implemented')

    def put(self, data, channel=None):
        self.get_queue(channel or self.DEFAULT_CHANNEL).put(data)

    def get(self, channel=None):
        return self.get_queue(channel or self.DEFAULT_CHANNEL).get()


class OutputChannelCollection(CommunicationChannelCollection):
    DEFAULT_CHANNEL = STDOUT
    ERROR_CHANNEL = STDERR

    def __init__(self, channels=(STDOUT, STDERR, )):
        super(OutputChannelCollection, self).__init__(channels)

    def get(self, channel=None):
        raise IOError('This channel is write-only, as a well disciplined output.')

    def put_error(self, error):
        raise error
        raise NotImplementedError('todo, error management')


class InputChannelCollection(CommunicationChannelCollection):
    DEFAULT_CHANNEL = STDIN

    def __init__(self, channels=(STDIN, )):
        super(InputChannelCollection, self).__init__(channels)

    def put(self, data, channel=None):
        raise IOError('This channel is read-only, as a well disciplined input.')

    def plug(self, channel_collection, channel=0, channel_from=0):
        self.set_queue(channel_collection.get_queue(channel_from), channel)

    def get_any(self):
        # todo documentation says .enpty() value is not reliable ... XXX
        while not self.terminated:
            for queue in self.queues:
                if not queue.empty():
                    return queue.get()
            time.sleep(0.5)

        raise TerminatedInputError('This input collection is terminated.')

    @property
    def terminated(self):
        for queue in self.queues:
            if queue and not queue.terminated:
                return False
            return True


