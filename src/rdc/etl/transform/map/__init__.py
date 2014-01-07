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

"""
Maps are transforms that will yield rows depending on the value of one input field. In association with ``FileExtract``
for example, it can parse the file content format and yield rows that have an added knowledge.

By default, maps use the topic (`_`) field for input

"""

from rdc.etl import DEFAULT_FIELD
from rdc.etl.error import AbstractError
from rdc.etl.io import STDIN
from rdc.etl.transform import Transform


class Map(Transform):
    """Base class for mappers.

    .. attribute:: map

        Map logic callable. Takes the hash's field value and yields iterable data.

    .. attribute:: field

        The input field.


    Example::

        >>> from rdc.etl.transform.map import Map
        >>> from rdc.etl.transform.util import clean

        >>> @Map
        ... def my_map(s_in):
        ...     for l in s_in.split('\\n'):
        ...        yield {'f%d' % i: v for i, v in enumerate(l.split(':'))}

        >>> map(clean, my_map({'_': 'a:b:c\\nb:c:d\\nc:d:e'}))
        [H{'f0': 'a', 'f1': 'b', 'f2': 'c'}, H{'f0': 'b', 'f1': 'c', 'f2': 'd'}, H{'f0': 'c', 'f1': 'd', 'f2': 'e'}]

    """

    field = DEFAULT_FIELD

    def __init__(self, map=None, field=None):
        super(Map, self).__init__()

        self.map = map or self.map
        self.field = field or self.field

    def map(self, value):
        raise AbstractError(self.map)

    def transform(self, hash, channel=STDIN):
        for line in self.map(hash[self.field]):
            yield hash.copy(line)

