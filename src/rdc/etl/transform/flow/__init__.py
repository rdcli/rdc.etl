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

def default_comparator(a, b):
    if a == b: return 0
    if a < b: return -1
    if a > b: return 1

def default_merger(a, b):
    """Create a copy of first argument and update with second argument's value."""
    c = copy(a).update(b)
    return c

def get_lower(comparator, a, b):
    if comparator(a, b) > 0:
        return b
    return a

def get_higher(comparator, a, b):
    if comparator(a, b) > 0:
        return a
    return b

# TODO XXX isn't there some better algorithm for this ?
def insert_sorted(comparator, lst, key, value, start=0, end=None):
    # initial case, end is the end of list.
    if end is None:
        end = len(lst)

    if start == end:
        # 0 sized range
        lst.insert(start, (key, value, ))
        return start

    # compute length. end boundary excluded out of range
    length = end - start

    # find pivot
    pivot = start + length // 2

    # compare key with pivot's key
    side = comparator(key, lst[pivot][0])

    # equality ?
    if side == 0:
        lst.insert(pivot, (key, value, ))
        return pivot

    # left side ?
    if side == -1:
        # special case, only one value to consider (the pivot). Insert on the left.
        if length == 1:
            lst.insert(start, (key, value, ))
            return start

        # otherwise, consider the left hand interval (excluding pivot)
        return insert_sorted(comparator, lst, key, value, start, pivot)

    # right side ?
    if side == 1:
        # end of our sorted list ? append.
        if len(lst) <= pivot + 1:
            lst.append((key, value, ))
            return len(lst) - 1

        # special case, only one value to consider (the pivot). Insert on the right.
        if length == 1:
            lst.insert(start + 1, (key, value, ))
            return start + 1

        # otherwise, consider the right hand interval (excluding pivot)
        return insert_sorted(comparator, lst, key, value, pivot + 1, end)

    # wtf ?
    raise ValueError('Invalid comparator return value %r.' % (side, ))
