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
from rdc.etl.transform import Transform


class SimpleFilter(Transform):
    """Filter out hashes from the stream depending on the :attr:`filter` callable return value, when called with the
    current hash as parameter

    """

    #: A callable used to filter the hashes.
    filter = None

    def __init__(self, filter=None):
        super(SimpleFilter, self).__init__()
        self.filter = filter or self.filter

    def transform(self, hash, channel=STDIN):
        if not self.filter or not callable(self.filter):
            raise RuntimeError('No callable provided to ' + self.__class__.__name__ + '.')

        if self.filter(hash):
            yield hash

