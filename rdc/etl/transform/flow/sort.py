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

from rdc.etl.io import STDIN
from rdc.etl.transform import Transform
from rdc.etl.transform.flow import default_comparator, insert_sorted


class Sort(Transform):
    """
    Fast'n'dirty implementation of a software "sort" transform. As we need to stack up all the stream rows in our
    instance before we can start freeing them, this is most of the time a bad idea to use this. Always prefer to sort
    your rows the earliest possible, for example in a DatabaseExtract.

    This still can be an acceptable solution for proof of concepts or small streams of data.

    An example of why you would like to sort data is for streams that _requires_ data to be sorted, for example a double
    input joiner (not existing at the time of writing this).

    :attr:`key`
        A tuple of keys on which to sort data.

    """

    comparator = None

    def __init__(self, key, comparator=None):
        super(Sort, self).__init__()

        self.key = key
        self.comparator = comparator or default_comparator

    def initialize(self):
        self._sorted = []

    def transform(self, hash, channel=STDIN):
        key = hash.get_values(self.key)
        insert_sorted(self.comparator, self._sorted, key, hash)

    def finalize(self):
        for key, value in self._sorted:
            yield value

