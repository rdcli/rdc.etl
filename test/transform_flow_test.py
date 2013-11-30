# -*- coding: utf-8 -*-
#
# Author: Romain Dorgueil <romain@dorgueil.net>
# Copyright: © 2011-2013 SARL Romain Dorgueil Conseil
#

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
