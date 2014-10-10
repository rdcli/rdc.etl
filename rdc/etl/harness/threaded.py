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
from Queue import Empty

import datetime
import threading
import time
import traceback
from rdc.etl import TICK, STATUS_PERIOD
from rdc.etl.harness.base import BaseHarness
from rdc.etl.hash import Hash
from rdc.etl.io import InactiveReadableError, IO_TYPES, DEFAULT_INPUT_CHANNEL, DEFAULT_OUTPUT_CHANNEL, Begin, End, \
    STDERR
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


class TransformThread(threading.Thread):
    """Thread encapsulating a transformation. Handle errors."""

    __thread_counter = _IntSequenceGenerator()

    def __init__(self, transform, group=None, target=None, name=None, args=(), kwargs=None, verbose=None):
        super(TransformThread, self).__init__(group, target, name, args, kwargs, verbose)
        self.transform = transform
        self.__thread_number = self.__class__.__thread_counter.next()
        self._stop = threading.Event()

    def handle_error(self, exc, tb):
        if STDERR in self.transform.OUTPUT_CHANNELS:
            self.transform._output.put(({
                'transform': self.transform,
                'exception': exc,
                'traceback': tb,
                }, STDERR, ))
        else:
            print str(exc) + '\n\n' + tb + '\n\n\n\n'

    @property
    def name(self):
        return self.transform.__name__ + '-' + str(self.__thread_number)

    def run(self):
        while not self.stopped:
            try:
                self.transform.step()
            except KeyboardInterrupt as e:
                raise
            except InactiveReadableError as e:
                # Terminated, exit loop.
                break
            except Empty as e:
                continue
            except Exception as e:
                self.handle_error(e, traceback.format_exc())

        try:
            self.transform.step(finalize=True)
        except (Empty, InactiveReadableError) as e:
            # Obviously, ignore.
            pass
        except Exception, e:
            self.handle_error(e, traceback.format_exc())

    def stop(self):
        self._stop.set()
        time.sleep(2)
        # xxx I do not want to do this. But really, what are my options ?
        self._Thread__stop()

    @property
    def stopped(self):
        return self._stop.isSet()

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
        self._transform_indexes = {}
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
        """Returns attached threads."""
        return self._threads.items()

    def get_transforms(self):
        """Returns attached transorms."""
        return self._transforms.items()

    def add(self, transform):
        """Register a transformation, create a thread object to manage its future lifecycle."""
        t_ident = id(transform)

        if not t_ident in self._transform_indexes:
            id_ = self._current_id.next()
            self._transforms[id_] = transform
            self._transform_indexes[t_ident] = id_
            self._threads[id_] = TransformThread(transform)

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

        status_index = 0
        interrupted = False
        # main loop until all threads are done
        while True:
            try:
                is_alive = False
                for id, thread in self._threads.items():
                    is_alive = is_alive or thread.is_alive()

                # communicate with the world
                if status_index <= 0:
                    for status in self.status:
                        status.update(self, debug=self.debug, profile=self.profile)
                    status_index = STATUS_PERIOD

                status_index -= 1

                # exit point
                if not is_alive:
                    break

                # take a nap. Time here determine how often status is updated, and the maximum waste of time after all
                # threads finished.
                time.sleep(TICK)
            except KeyboardInterrupt as e:
                interrupted = True
                for id, thread in self._threads.items():
                    if thread.is_alive():
                        thread.stop()
                break

        # run finalization methods for statuses
        for status in self.status:
            status.finalize(self, debug=self.debug, profile=self.profile)
        if interrupted:
            print 'Caught keyboard interrupt (Ctrl-C), stopping threads ...'

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

        # fluid api
        return self

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


