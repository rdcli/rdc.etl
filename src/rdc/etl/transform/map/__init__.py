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

"""
Maps are transforms that will yield rows depending on the value of one input field. In association with ``FileExtract``
for example, it can parse the file content format and yield rows that have an added knowledge.

By default, maps use the topic (`_`) field for input

"""

from rdc.etl.io import STDIN
from rdc.etl.transform import Transform


class Map(Transform):
    """Base class for mappers.

    .. attribute:: mapper

        Map logic callable. Takes the hash's field value and yields iterable data.

    .. attribute:: field

        The input field.

    """

    field = '_'

    def __init__(self, mapper=None, field=None):
        super(Map, self).__init__()

        self.mapper = mapper or self.mapper
        self.field = field or self.field

    def mapper(self, value):
        raise NotImplementedError('Abstract.')

    def transform(self, hash, channel=STDIN):
        for line in self.mapper(hash[self.field]):
            yield hash.copy(line)

