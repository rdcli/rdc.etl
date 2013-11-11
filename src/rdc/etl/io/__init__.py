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

from copy import copy
import time
from rdc.etl.hash import Hash
from Queue import Queue

STDIN = 0
STDIN2 = 1
STDOUT = 0
STDERR = -1
STDOUT2 = 1
DEFAULT_INPUT_CHANNEL = STDIN
DEFAULT_OUTPUT_CHANNEL = STDOUT
INPUT_TYPE = 'input'
OUTPUT_TYPE = 'output'


class Token(object):
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return '<%s>' % (self.name, )

Begin = Token('Begin')
End = Token('End')
BUFFER_SIZE = 8192

class IReadable(object):
    @abstract
    def get(self, block=True, timeout=True):
        pass

class IWritable(object):
    @abstract
    def put(self, data, block=True, timeout=True):
        pass

class InactiveIOError(IOError):
    pass


class InactiveReadableError(InactiveIOError):
    pass


class InactiveWritableError(InactiveIOError):
    pass


class InputMultiplexer(IReadable):
    def __init__(self, channels):
        self.queues = dict([(channel, Input()) for channel in channels])
        self._plugged = set()

    def get(self, block=True, timeout=True):
        """Gets a (data, channel) tuple from the first queue ready for it.
        """
        # todo documentation says .enpty() value is not reliable ... XXX
        while self.alive:
            for id, queue in self.queues.items():
                if not queue.empty():
                    # xxx useage of block/timeout here is wrong
                    data = queue.get(block, timeout)
                    return data, id
            # todo xxx take block and timeout into account
            time.sleep(0.2)

        raise InactiveReadableError('InputMultiplexer is terminated.')

    def plug(self, dmux, channel=DEFAULT_INPUT_CHANNEL, dmux_channel=DEFAULT_OUTPUT_CHANNEL):
        dmux.plug_into(self.queues[channel], channel=dmux_channel)
        self._plugged.add(channel)

    @property
    def alive(self):
        for channel, queue in self.queues.items():
            if queue.alive:
                return True
        return False

    @property
    def plugged(self):
        return [queue for channel, queue in self.queues.items() if channel in self._plugged]

    @property
    def unplugged(self):
        return [queue for channel, queue in self.queues.items() if channel not in self._plugged]


class OutputDemultiplexer(IWritable):
    def __init__(self, channels):
        self.channels = dict([(channel, []) for channel in channels])

    def put(self, data, block=True, timeout=None):
        data, channel = self.__demux(data)

        if not channel in self.channels:
            raise IOError('Unknown channel')

        for target in self.channels[channel]:
            target.put(isinstance(data, Token) and data or copy(data), block, timeout)

    def put_all(self, data, block=True, timeout=None):
        for channel in self.channels:
            self.put((data, channel, ), block, timeout)

    def plug_into(self, target, channel):
        if not channel in self.channels:
            raise IOError('Unknown channel %r.' % (channel, ))
        if target in self.channels[channel]:
            raise ValueError('Channel already have this target plugged for channel %r.' % (channel, ))
        self.channels[channel].append(target)

    def __demux(self, data):
        if isinstance(data, Hash):
            return data, DEFAULT_OUTPUT_CHANNEL
        if len(data) == 1:
            return data, DEFAULT_OUTPUT_CHANNEL
        if len(data) == 2:
            return data[0], data[1]
        raise ValueError('Unintelligible message.')


class Input(IReadable, IWritable, Queue):
    def __init__(self, maxsize=BUFFER_SIZE):
        Queue.__init__(self, maxsize)
        self._runlevel = 0
        self._writable_runlevel = 0

    def put(self, data, block=True, timeout=None):
        # Begin token is a metadata to raise the input runlevel.
        if data == Begin:
            self._runlevel += 1
            self._writable_runlevel += 1
            return

        # Check we are actually able to receive data.
        if self._writable_runlevel < 1:
            raise InactiveWritableError('Cannot put() on an inactive IWritable.')

        if data == End:
            self._writable_runlevel -= 1

        return Queue.put(self, data, block, timeout)

    def get(self, block=True, timeout=None):
        if not self.alive:
            raise InactiveReadableError('Cannot get() on an inactive IReadable.')

        data = Queue.get(self, block, timeout)

        if data == End:
            self._runlevel -= 1
            if not self.alive:
                raise InactiveReadableError('Cannot get() on an inactive IReadable (runlevel just reached 0).')
            return self.get(block, timeout)

        return data

    @property
    def alive(self):
        return self._runlevel > 0


IO_TYPES = {
    INPUT_TYPE: InputMultiplexer,
    OUTPUT_TYPE: OutputDemultiplexer,
}

