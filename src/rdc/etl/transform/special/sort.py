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


class Sort(Transform):
    """
    Fast'n'dirty implementation of a software "sort" transform. As we need to stack up all the stream rows in our
    instance before we can start freeing them, this is most of the time a bad idea to use this. Always prefer to sort
    your rows the earliest possible, for example in a DatabaseExtract.

    This still can be an acceptable solution for proof of concepts or small streams of data.

    An example of why you would like to sort data is for streams that _requires_ data to be sorted, for example a double
    input joiner (not existing at the time of writing this).

    TODO the sort method (__insert_in_place) looks really crappy, rewrite this.

    :attr:`key`
        A tuple of keys on which to sort data.

    """

    def __init__(self, key, comparator=None):
        super(Sort, self).__init__()

        self.key = key
        self.comparator = comparator or self.comparator

    def comparator(self, a, b):
        if a == b:
            return 0
        if a < b:
            return -1
        if a > b:
            return 1

    def initialize(self):
        self._sorted = []

    def transform(self, hash, channel=STDIN):
        key = hash.get_values(self.key)
        self.__insert_in_place(key, hash)

    def finalize(self):
        for key, value in self._sorted:
            yield value

    def __insert_in_place(self, key, value, start=0, length=-1):
        # initial case, length to consider is the whole list
        if length == -1: length = len(self._sorted)

        # if length to consider is 0, the start position is our insert point
        if length == 0:
            self._sorted.insert(start, (key, value))
            return

        # find pivot
        middle = start + length // 2

        # compare key with pivot's key
        side = self.comparator(key, self._sorted[middle][0])

        # equality ?
        if side == 0:
            self._sorted.insert(middle, (key, value))
            return

        # left side ?
        if side == -1:
            # special case, only one value to consider (the pivot). Insert on the left.
            if length == 1:
                self._sorted.insert(start, (key, value))
                return

            # otherwise, consider the left hand interval (excluding pivot)
            self.__insert_in_place(key, value, start, middle - start)
            return

        # right side ?
        if side == 1:
            # end of our sorted list ? append.
            if len(self._sorted) <= middle + 1:
                self._sorted.append((key, value))
                return

            # special case, only one value to consider (the pivot). Insert on the right.
            if length == 1:
                self._sorted.insert(start + 1, (key, value))
                return

            # otherwise, consider the right hand interval (excluding pivot)
            self.__insert_in_place(key, value, middle + 1, length - (middle - start + 1))
            return

        # wtf ?
        raise ValueError('Invalid comparator return value.')



