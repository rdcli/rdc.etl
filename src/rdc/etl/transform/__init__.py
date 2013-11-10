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
from rdc.etl.io import STDIN, STDOUT, STDERR, InputQueueCollection, OutputQueueCollection, TerminatedInputError, EndOfStream


class Transform(object):
    INPUT_CHANNELS = (STDIN, )
    OUTPUT_CHANNELS = (STDOUT, STDERR, )

    def __init__(self):
        self._s_in = 0
        self._s_out = 0

        self._input = InputQueueCollection(self.INPUT_CHANNELS)
        self._output = OutputQueueCollection(self.OUTPUT_CHANNELS)

        self._initialized = False
        self._finalized = False

    @property
    def virgin(self):
        return not self._initialized and not self._finalized

    def step(self, finalize=False):
        if not self._initialized:
            self._initialized = True
            self.__execute_and_handle_output(self.initialize)

        try:
            # Pull data from the first available input channel (blocking)
            hash, channel = self._input.get_any()
            self._s_in += 1
            # Execute actual transformation
            self.__execute_and_handle_output(self.transform, hash, channel)
        except TerminatedInputError, e:
            pass
        except Exception, e:
            raise
        finally:
            if finalize and not self._finalized:
                self._finalized = True
                self.__execute_and_handle_output(self.finalize)
                self._output.put_all(EndOfStream)

    @abstract
    def transform(self, hash, channel=STDIN):
        """Core transformation method that will be called for each input data row."""
        pass

    def initialize(self):
        """If you need to execute code before any item is transformed, this is the place."""
        pass

    def finalize(self):
        """If you need to execute code after all items are transformed, this is the place. It's especially usefull for
        buffering transformations, or other blocking types."""
        pass

    def __repr__(self):
        return '<' + self.get_name() + ' ' + self.get_stats_as_string() + '>'

    def get_name(self):
        return  self.__class__.__name__

    def get_stats(self):
        return OrderedDict((
            ('in', self._s_in),
            ('out', self._s_out),
        ))

    def get_stats_as_string(self):
        return ' '.join(['%s=%d' % (k, v) for k, v in self.get_stats().items()])

    def __execute_and_handle_output(self, callable, *args, **kwargs):
        """Runs a transformation callable with given args/kwargs and flush the result into the right
        output channel.
        """
        results = callable(*args, **kwargs)
        # Put data onto output channels
        if isinstance(results, types.GeneratorType):
            for result in results:
                # todo better stats
                self._s_out += 1
                data, channel = self.__normalize_output(result)
                self._output.put(data, channel=channel)
        elif results is not None:
            self._s_out += 1
            data, channel = self.__normalize_output(results)
            self._output.put(data, channel=channel)

    def __normalize_output(self, result):
        """Take one of the various formats allowed as return value from transformation callables
        (initialize/transform/finalize) and returns a 2 element tuple (data, channel).
        """
        if isinstance(result, Hash):
            data, channel = result, self.OUTPUT_CHANNELS[0]
        elif len(result) == 1:
            data, channel = result[0], self.OUTPUT_CHANNELS[0]
        elif len(result) != 2:
            raise IOError('Unsupported output format from transform().')
        else:
            data, channel = result

        if channel not in self.OUTPUT_CHANNELS:
            raise IOError('Invalid channel %r selected for transform() output.' % (channel, ))

        return data, channel

