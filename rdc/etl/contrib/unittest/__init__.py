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
from rdc.etl.hash import Hash
from rdc.etl.transform.util import clean

class BaseTestCase(unittest.TestCase):
    def assertStreamEqual(self, first, second, msg=None):
        first = map(clean, first)
        second = map(clean, second)
        self.assertEqual(len(first), len(second), msg)
        for i in xrange(0, len(first)):
            left = first[i]
            right = second[i]
            self.assertEqual(left.items(), right.items(), msg)

