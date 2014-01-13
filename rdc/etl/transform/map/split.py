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

from copy import copy
from rdc.etl import DEFAULT_FIELD
from rdc.etl.error import AbstractError
from rdc.etl.io import STDIN
from rdc.etl.transform import Transform


class SplitMap(Transform):
    """
    Split using a function.

    .. stability:: alpha

    """

    field = DEFAULT_FIELD
    _output_field = None

    def __init__(self, split=None, field=None, output_field=None):
        super(SplitMap, self).__init__()

        self.split = split or self.split
        self.field = field or self.field
        self._output_field = output_field or self._output_field

    @property
    def output_field(self):
        return self._output_field or self.field

    @output_field.setter
    def output_field(self, value):
        self._output_field = value

    def split(self, field):
        raise AbstractError(self.split)

    def transform(self, hash, channel=STDIN):
        values = self.split(hash[self.field])
        del hash[self.field]
        for value in values:
            hash = copy(hash)
            hash[self.output_field] = value
            yield hash

