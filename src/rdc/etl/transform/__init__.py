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
# todo make this python2.6 compatible
from collections import OrderedDict
from rdc.etl.hash import Hash
from rdc.etl.io import STDIN, STDOUT, STDERR, InputMultiplexer, OutputDemultiplexer, InactiveReadableError, Begin, End


class Transform(object):
    INPUT_CHANNELS = (STDIN, )
    OUTPUT_CHANNELS = (STDOUT, STDERR, )

    def __init__(self):
        self._s_in = 0
        self._s_out = 0

        self._input = InputMultiplexer(self.INPUT_CHANNELS)
        self._output = OutputDemultiplexer(self.OUTPUT_CHANNELS)

        self._initialized = False
        self._finalized = False

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

    @abstract
    def transform(self, hash, channel=STDIN):
        """Core transformation method that will be called for each input data row."""
        pass

    def finalize(self):
        """If you need to execute code after all items are transformed, this is the place. It's especially usefull for
        buffering transformations, or other blocking types."""
        pass

    @property
    def virgin(self):
        return not self._initialized and not self._finalized

    def get_name(self):
        return  self.__class__.__name__

    def get_stats(self):
        return OrderedDict((
            ('in', self._s_in),
            ('out', self._s_out),
        ))

    def get_stats_as_string(self):
        return ' '.join(['%s=%d' % (k, v) for k, v in self.get_stats().items()])


    def __repr__(self):
        return '<' + self.get_name() + ' ' + self.get_stats_as_string() + '>'

    def __execute_and_handle_output(self, callable, *args, **kwargs):
        """Runs a transformation callable with given args/kwargs and flush the result into the right
        output channel.
        """
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

