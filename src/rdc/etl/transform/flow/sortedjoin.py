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

from functools import partial
from rdc.etl.io import STDIN, STDIN2
from rdc.etl.transform import Transform
from rdc.etl.transform.flow import default_comparator, get_lower, insert_sorted, default_merger

class SortedJoin(Transform):
    INPUT_CHANNELS = (STDIN, STDIN2, )
    is_outer = False

    def __init__(self, key, merger=None, comparator=None, is_outer=None):
        super(SortedJoin, self).__init__()

        self.key = key
        self.merger = merger or default_merger
        self.comparator = comparator or default_comparator
        self.is_outer = is_outer or self.is_outer

    def initialize(self):
        self._minimum = None
        self._channel_value = dict(((c, None) for c in self.INPUT_CHANNELS))
        self._sorted = {
            STDIN: list(),
            STDIN2: list(),
        }

    def _get_minimum_value(self):
        return reduce(partial(get_lower, self.comparator), self._channel_value)

    def transform(self, hash, channel=STDIN):
        current_key = hash.get_values(self.key)
        insert_sorted(self.comparator, self._sorted[channel], current_key, hash)
        for data in self.consume():
            yield data

    def consume(self):
        while len(self._sorted[STDIN]) and len(self._sorted[STDIN2]):
            side = self.comparator(self._sorted[STDIN2][0][0], self._sorted[STDIN][0][0])

            if side == 0:
                # match, merge
                _key, _data = self._sorted[STDIN2].pop(0)
                pos = 0
                while (pos < (len(self._sorted[STDIN]) - 1)) and (self.comparator(self._sorted[STDIN][pos][0], _key) == 0):
                    self.merger(_data, self._sorted[STDIN][pos][1])
                    pos += 1
            elif side == -1:
                # no match
                if self.is_outer:
                    yield self._sorted[STDIN2].pop(0)[1]
                else:
                    self._sorted[STDIN2].pop(0)
            elif side == 1:
                # passed, yield possible
                yield self._sorted[STDIN].pop(0)[1]
            else:
                raise RuntimeError('invalid comparator return value')

    def finalize(self):
        for data in self.consume():
            yield data
        while len(self._sorted[STDIN]):
            yield self._sorted[STDIN].pop(0)[1]

