# -*- coding: utf-8 -*-
#
# copyright 2012-2014 romain dorgueil
#
# licensed under the apache license, version 2.0 (the "license");
# you may not use this file except in compliance with the license.
# you may obtain a copy of the license at
#
#     http://www.apache.org/licenses/license-2.0
#
# unless required by applicable law or agreed to in writing, software
# distributed under the license is distributed on an "as is" basis,
# without warranties or conditions of any kind, either express or implied.
# see the license for the specific language governing permissions and
# limitations under the license.

import datetime
import time
import traceback
from threading import Thread
from rdc.etl import TICK
from rdc.etl.harness.base import BaseHarness
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
        return self.transform.__name__ + '-' + str(self.__thread_number)

    def run(self):
        while True:
            try:
                self.transform.step()
            except KeyboardInterrupt as e:
                # todo send signal to harness, clean stop required. Needed ? read about threads and interrupt, only main may receive this.
                break
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


class ThreadedHarness(BaseHarness):
    """Builder for ETL job python callables, using threads for parallelization."""

    def __init__(self, debug=False, profile=False):
        super(ThreadedHarness, self).__init__()
        self.debug = debug
        self.profile = profile
        self.status = []
        self._transforms = {}
        self._threads = {}
        self._current_id = _IntSequenceGenerator()
        self._started_at = None
        self._finished_at = None

    def __call__(self):
        self._started_at = datetime.datetime.now()
        result = super(ThreadedHarness, self).__call__()
        self._finished_at = datetime.datetime.now()
        return result

    def get_threads(self):
        return self._threads.items()

    def get_transforms(self):
        return self._transforms.items()

    def add(self, transform):
        """Register a transformation, create a thread object to manage its future lifecycle."""
        id = self._current_id.next()
        self._transforms[id] = transform
        self._threads[id] = TransformThread(transform)
        return transform # BC, maybe id would be a better thing to return (todo 2.0, or even 1.0 before api freeze)

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

        # run initialization methods for statuses
        for status in self.status:
            status.initialize(self, debug=self.debug, profile=self.profile)

        # main loop until all threads are done
        while True:
            try:
                is_alive = False
                for id, thread in self._threads.items():
                    is_alive = is_alive or thread.is_alive()

                # communicate with the world
                for status in self.status:
                    status.update(self, debug=self.debug, profile=self.profile)

                # exit point
                if not is_alive:
                    break

                # take a nap. Time here determine how often status is updated, and the maximum waste of time after all
                # threads finished.
                time.sleep(TICK)
            except KeyboardInterrupt as e:
                # todo cleaner stop ?
                break

        # run finalization methods for statuses
        for status in self.status:
            status.finalize(self, debug=self.debug, profile=self.profile)

        # Wait for all transform threads to die
        for id, thread in self._threads.items():
            thread.join()


    def add_chain(self, *transforms, **kwargs):
        """Main helper method to add chains of transforms to this harness. You can plug the whole chain from and to
        other transforms by specifying `input` and `output` parameters.

        The transforms provided should not be bound yet.

        >>> h = ThreadedHarness()
        >>> t1, t2, t3 = Transform(), Transform(), Transform()
        >>> h.add_chain(t1, t2, t3)

        """
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


