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

import unittest
from rdc.etl.transform.flow import insert_sorted, default_comparator


class TransformFlowTestCase(unittest.TestCase):
    def test_insert_sorted(self):
        l = []

        # empty list
        insert_sorted(default_comparator, l, 'mmm', 'mmm')
        self.assertEqual(l, [('mmm', 'mmm', )])

        # append
        insert_sorted(default_comparator, l, 'nnn', 'nnn')
        self.assertEqual(l, [('mmm', 'mmm', ), ('nnn', 'nnn', )])

        # append
        insert_sorted(default_comparator, l, 'ooo', 'ooo')
        self.assertEqual(l, [('mmm', 'mmm', ), ('nnn', 'nnn', ), ('ooo', 'ooo', )])

        # before the last
        insert_sorted(default_comparator, l, 'nzz', 'nzz')
        self.assertEqual(l, [('mmm', 'mmm', ), ('nnn', 'nnn', ), ('nzz', 'nzz', ), ('ooo', 'ooo', )])

        # second
        insert_sorted(default_comparator, l, 'naa', 'naa')
        self.assertEqual(l, [('mmm', 'mmm', ), ('naa', 'naa', ), ('nnn', 'nnn', ), ('nzz', 'nzz', ), ('ooo', 'ooo', )])

        # first
        insert_sorted(default_comparator, l, 'aaa', 'aaa')
        self.assertEqual(l, [('aaa', 'aaa', ), ('mmm', 'mmm', ), ('naa', 'naa', ), ('nnn', 'nnn', ), ('nzz', 'nzz', ), ('ooo', 'ooo', )])


if __name__ == '__main__':
    unittest.main()
