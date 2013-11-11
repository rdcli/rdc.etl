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

from rdc.etl.io import STDOUT, STDOUT2, STDERR, STDIN

from rdc.etl.transform import Transform


class Split(Transform):
    """Depending on the result of output_selector(), routes the input rows to various output channels."""

    OUTPUT_CHANNELS = (STDOUT, STDOUT2, STDERR, )

    def __init__(self, output_selector = None):
        super(Split, self).__init__()

        if output_selector:
            self.select_output = output_selector

    def output_selector(self, hash):
        return STDOUT

    def transform(self, hash, channel=STDIN):
        output_channel = self.select_output(hash)

        if not output_channel in self.OUTPUT_CHANNELS:
            raise IOError('Selected output channel %r does not exist.' % (output_channel, ))

        yield hash, output_channel
