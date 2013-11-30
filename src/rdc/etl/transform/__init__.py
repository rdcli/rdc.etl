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

import types
from abc import ABCMeta, abstractmethod
# todo make this python2.6 compatible
from collections import OrderedDict
from rdc.etl.hash import Hash, H
from rdc.etl.io import STDIN, STDOUT, STDERR, InputMultiplexer, OutputDemultiplexer, End

class ITransform:
    __metaclass__ = ABCMeta

    @abstractmethod
    def transform(self, hash, channel=STDIN):
        """All input rows that comes to one of this transform's input channels will be passed to this method. If you
        only have one input channel, you can safely ignore the channel value, although you'll need it in method
        prototype."""

class Transform(ITransform):
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

    def __init__(self, transform=None, input_channels=None, output_channels=None):
        self.transform = transform or self.transform
        self.INPUT_CHANNELS = input_channels or self.INPUT_CHANNELS
        self.OUTPUT_CHANNELS = output_channels or self.OUTPUT_CHANNELS

        self._s_in = 0
        self._s_out = 0

        self._input = InputMultiplexer(self.INPUT_CHANNELS)
        self._output = OutputDemultiplexer(self.OUTPUT_CHANNELS)

        self._initialized = False
        self._finalized = False

        self._name = self.__class__.__name__

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
        raise NotImplementedError('Abstract.')

    # IO related

    def step(self, finalize=False):
        if not self._initialized:
            self._initialized = True
            self.__execute_and_handle_output(self.initialize)

        try:
            # Pull data from the first available input channel (blocking)
            data, channel = self._input.get()
            self._s_in += 1
            # Execute actual transformation
            self.__execute_and_handle_output(self.transform, data, channel)
        finally:
            if finalize and not self._finalized:
                self._finalized = True
                self.__execute_and_handle_output(self.finalize)
                self._output.put_all(End)

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
        return self._name

    @__name__.setter
    def __name__(self, value):
        self._name = value

    def get_stats(self):
        return OrderedDict((
            ('in', self._s_in),
            ('out', self._s_out),
        ))

    def get_stats_as_string(self):
        return ' '.join(['%s=%d' % (k, v) for k, v in self.get_stats().items()])

    def __repr__(self):
        return '<' + self.__name__ + ' ' + self.get_stats_as_string() + '>'

    # Private

    def __execute_and_handle_output(self, callable, *args, **kwargs):
        """Runs a transformation callable with given args/kwargs and flush the result into the right
        output channel."""
        results = callable(*args, **kwargs)
        # Put data onto output channels
        if isinstance(results, types.GeneratorType):
            for data in results:
                # todo better stats
                self._s_out += 1
                self._output.put(data)
        elif results is not None:
            self._s_out += 1
            self._output.put(results)

