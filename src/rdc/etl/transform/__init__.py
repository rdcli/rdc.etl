# -*- coding: utf-8 -*-
#
# Copyright 2012-2014 Romain Dorgueil
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

import itertools
import types
from abc import ABCMeta, abstractmethod
from rdc.etl import H
from rdc.etl.error import AbstractError
from rdc.etl.hash import Hash
from rdc.etl.io import STDIN, STDOUT, STDERR, InputMultiplexer, OutputDemultiplexer, End
from rdc.etl.stat import Statisticable
from rdc.etl.util import Timer


class ITransform:
    __metaclass__ = ABCMeta

    @abstractmethod
    def transform(self, hash, channel=STDIN):
        """All input rows that comes to one of this transform's input channels will be passed to this method. If you
        only have one input channel, you can safely ignore the channel value, although you'll need it in method
        prototype."""
        raise AbstractError(self.transform)


class Transform(ITransform, Statisticable):
    """Base class and decorator for transformations.

    .. automethod:: transform

    .. attribute:: INPUT_CHANNELS

        List of input channel names.

    .. attribute:: OUTPUT_CHANNELS

        List of output channel names

    Example::

        >>> @Transform
        ... def my_transform(hash, channel=STDIN):
        ...     yield hash.copy({'foo': hash['foo'].upper()})

        >>> print list(my_transform(
        ...         H(('foo', 'bar'), ('bar', 'alpha')),
        ...         H(('foo', 'baz'), ('bar', 'omega')),
        ...     ))
        [H{'foo': 'BAR', 'bar': 'alpha'}, H{'foo': 'BAZ', 'bar': 'omega'}]

    """

    INPUT_CHANNELS = (STDIN, )
    OUTPUT_CHANNELS = (STDOUT, STDERR, )
    _name = None

    def __init__(self, transform=None, input_channels=None, output_channels=None):
        # Use the callable name if provided
        if transform and not self._name:
            self._name = transform.__name__

        self.INPUT_CHANNELS = input_channels or self.INPUT_CHANNELS
        self.OUTPUT_CHANNELS = output_channels or self.OUTPUT_CHANNELS

        self._input = InputMultiplexer(self.INPUT_CHANNELS)
        self._output = OutputDemultiplexer(self.OUTPUT_CHANNELS)
        self._exec_time = 0.0
        self._exec_count = 0

        self._booted = False
        self._initialized = False
        self._finalized = False

        self.transform = transform or self.transform

    def __call__(self, *stream, **options):
        channel = options['channel'] if 'channel' in options else STDIN

        for hash in stream:
            if not isinstance(hash, Hash):
                hash = Hash(hash)

            for line in self.transform(hash, channel):
                yield line

    # ITransform implementation

    def transform(self, hash, channel=STDIN):
        """Core transformation method that will be called for each input data row."""
        raise AbstractError(self.transform)

    # IO related

    def step(self, finalize=False):
        if not self._booted:
            # todo find something to make this work
            self.boot()
            self._booted = True

        if not self._initialized:
            self._initialized = True
            self.__execute_and_handle_output(self.initialize)

        try:
            # Pull data from the first available input channel (blocking)
            data, channel = self._input.get()
            # Execute actual transformation
            self.__execute_and_handle_output(self.transform, data, channel)
        finally:
            if finalize and not self._finalized:
                self._finalized = True
                self.__execute_and_handle_output(self.finalize)
                self._output.put_all(End)

    def boot(self):
        """Just before transformation is started, validate everything is ready."""
        pass

    def initialize(self):
        """If you need to execute code before any item is transformed, this is the place."""
        pass

    def finalize(self):
        """If you need to execute code after all items are transformed, this is the place. It's especially usefull for
        buffering transformations, or other blocking types."""
        pass

    @property
    def virgin(self):
        """Whether or not this transformation already contains a yucca (spéciale dédicace)."""
        return not self._initialized and not self._finalized

    # Name, statistics and representation logic. This is basic but important, as it will serve visualisation/debugging
    # purpose.

    @property
    def __name__(self):
        return self._name or type(self).__name__

    @__name__.setter
    def __name__(self, value):
        self._name = value

    def get_local_stats(self, debug=False, profile=False):
        if profile:
            return (
                (u'τ', '%.2fs' % (self._exec_time, ), ),
                (u'ε', self._exec_count, ),
                (u'τ.ε⁻¹', ((self._exec_count > 0) and (u'%.1fms' % (1000 * self._exec_time / self._exec_count, )) or u'∞'), ),
                (u'ε.τ⁻¹', ((self._exec_time > 0) and (u'%.1f/s' % (self._exec_count / self._exec_time, )) or u'∞'), ),
            )
        return ()

    def get_stats(self, debug=False, profile=False):
        stats = itertools.chain(
            self._input.stats,
            self._output.stats,
            self.get_local_stats(debug=debug, profile=profile)
        )
        return (
            (name, stat, ) for name, stat in stats
        )


    def __repr__(self):
        return u'<{0} {1}>'.format(self.__name__, self.get_unicode_stats())

    # Private
    def __execute_and_handle_output(self, callable, *args, **kwargs):
        """Runs a transformation callable with given args/kwargs and flush the result into the right
        output channel."""

        timer = Timer()
        with timer:
            results = callable(*args, **kwargs)
        self._exec_time += timer.duration

        # Put data onto output channels
        if isinstance(results, types.GeneratorType):
            while True:
                timer = Timer()
                with timer:
                    try:
                        result = results.next()
                    except StopIteration as e:
                        break
                self._exec_time += timer.duration
                self._exec_count += 1
                self._output.put(result)
        elif results is not None:
            self._exec_count += 1
            self._output.put(results)
        else:
            self._exec_count += 1


