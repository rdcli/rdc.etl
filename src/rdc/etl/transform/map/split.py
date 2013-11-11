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

from rdc.etl.io import STDIN
import re
from rdc.etl.transform import Transform


class SplitMap(Transform):
    """"""
    field = '_'
    output_field = '_'
    delimiter = r'\s+'
    content = r'(.*)'
    skip = 0

    def __init__(self, field=None, output_field=None, delimiter=None, content=None, skip=None):
        super(SplitMap, self).__init__()

        self.field = field or self.field
        self.output_field = output_field or field
        self.delimiter = delimiter or self.delimiter
        self.content = content or self.content
        self.skip = skip or self.skip

        self._current = 0

    def transform(self, hash, channel=STDIN):
        s_in = hash.get(self.field)
        for value in re.split(self.delimiter, s_in[self._current:], flags=re.MULTILINE):
            if self.skip > 0:
                self.skip -= 1
                continue

            yield hash \
                .copy() \
                .remove(self.field) \
                .set(self.output_field, value)

