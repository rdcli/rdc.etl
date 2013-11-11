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
from threading import Thread
import traceback
from rdc.etl.harness import AbstractHarness
from rdc.etl.hash import Hash
from rdc.etl.io import InactiveReadableError, IO_TYPES, DEFAULT_INPUT_CHANNEL, DEFAULT_OUTPUT_CHANNEL, Begin, End
from rdc.etl.transform import Transform

class _IntSequenceGenerator(object):
    """Simple integer sequence generator."""

    def __init__(self):
        self.current = 0

    def get(self):
        return self.current

    def next(self):
        self.current += 1
        return self.current


class TransformThread(Thread):
    """Thread encapsulating a transformation. Handle errors."""

    __thread_counter = _IntSequenceGenerator()

    def __init__(self, transform, group=None, target=None, name=None, args=(), kwargs=None, verbose=None):
        super(TransformThread, self).__init__(group, target, name, args, kwargs, verbose)
        self.transform = transform
        self.__thread_number = self.__class__.__thread_counter.next()

    def handle_error(self, exc, tb):
        print str(exc) + '\n\n' + tb + '\n\n\n\n'

    @property
    def name(self):
        return self.transform.get_name() + '-' + str(self.__thread_number)

    def run(self):
        while True:
            try:
                self.transform.step()
            except InactiveReadableError, e:
                # Terminated, exit loop.
                break
            except Exception, e:
                self.handle_error(e, traceback.format_exc())

        try:
            self.transform.step(finalize=True)
        except InactiveReadableError, e:
            # Obviously, ignore.
            pass
        except Exception, e:
            self.handle_error(e, traceback.format_exc())

    def __repr__(self):
        """Adds "alive" information to the transform representation."""
        return (self.is_alive() and '+' or '-') + ' ' + self.name + ' ' + self.transform.get_stats_as_string()


class ThreadedHarness(AbstractHarness):
    """Builder for ETL job python callables, using threads for parallelization."""

    def __init__(self):
        super(ThreadedHarness, self).__init__()
        self._transforms = {}
        self._threads = {}
        self._current_id = _IntSequenceGenerator()

    def validate(self):
        """Validation of transform graph validity."""
        for id, transform in self._transforms.items():
            # Adds a special single empty hash queue to unplugged inputs
            for queue in transform._input.unplugged:
                queue.put(Begin)
                queue.put(Hash())
                queue.put(End)

            transform._output.put_all(Begin)

    def loop(self):
        """Starts all the thread and loop forever until they are all dead."""
        # todo healthcheck ? (cycles ... dead ends ... orphans ...)

        # start all threads
        for id, thread in self._threads.items():
            thread.start()

        # main loop until all threads are done
        while True:
            is_alive = False
            for id, thread in self._threads.items():
                is_alive = is_alive or thread.is_alive()

            # communicate with the world
            self.update_status()

            # exit point
            if not is_alive:
                break

            # take a nap. Time here determine how often status is updated, and the maximum waste of time after all
            # threads finished.
            time.sleep(0.2)

        # Wait for all transform threads to die
        for id, thread in self._threads.items():
            thread.join()

    def update_status(self):
        """Sends current status to all status objects. This API part is subject to changes."""
        for status in self.status:
            status.update(self._threads.values())

    # Methods below does not belong to API.
    def add(self, transform):
        """Register a transformation, create a thread object to manage its future lifecycle."""
        id = self._current_id.next()
        self._transforms[id] = transform
        self._threads[id] = TransformThread(transform)
        return transform # BC, maybe id would be a better thing to return (todo 2.0, or even 1.0 before api freeze)

    def add_chain(self, *transforms, **kwargs):
        if not len(transforms):
            raise Exception('At least one transform should be provided to form a chain.')

        input, output, input_channel, output_channel = None, None, None, None

        # Carefull! Input parameter should be an _output_ that we'll plug into our chain input.
        if 'input' in kwargs:
            input, input_channel = self.__find_output(kwargs['input'])

        # Carefull! Output parameter should be an _input_ that we'll plug into our chain input.
        if 'output' in kwargs:
            output, output_channel = self.__find_input(kwargs['output'])

        # Register the transformations and plug them together, as a chain.
        last_transform = None
        first_transform = transforms[0]
        for transform in transforms:
            if not transform.virgin:
                raise RuntimeError('You can\'t reuse a transform for now.')

            self.add(transform)

            if last_transform:
                self.__plug(last_transform._output, 0, transform._input, 0)

            last_transform = transform

        if input:
            # input contains the output of previous transform.
            self.__plug(input, input_channel, first_transform._input, 0)

        if output:
            # output contains the input we will plug our output into.
            self.__plug(last_transform._output, 0, output, output_channel)

    # Private stuff.

    def __find_input(self, mixed, default=DEFAULT_INPUT_CHANNEL):
        return self.__find_io_and_channel('input', mixed, default)

    def __find_output(self, mixed, default=DEFAULT_OUTPUT_CHANNEL):
        return self.__find_io_and_channel('output', mixed, default)

    def __find_io_and_channel(self, type, mixed, default=0):
        assert type in IO_TYPES, 'Invalid io type %r.' % (type, )

        if isinstance(mixed, Transform):
            io, channel = getattr(mixed, '_' + type), default
        elif isinstance(mixed, IO_TYPES[type]):
            io, channel = mixed, default
        elif len(mixed) == 1:
            io, channel = self.__find_io_and_channel(type, mixed[0], default)
        elif len(mixed) != 2:
            raise IOError('Unsupported %s given: %r.' % (type, mixed, ))
        else:
            io, channel = self.__find_io_and_channel(type, mixed[0], default=mixed[1])

        return io, channel

    def __plug(self, from_dmux, from_channel, to_mux, to_channel):
        to_mux.plug(from_dmux, channel=to_channel, dmux_channel=from_channel)


