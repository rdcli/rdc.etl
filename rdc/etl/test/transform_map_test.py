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
from collections import OrderedDict

import unittest
from rdc.etl.extra.unittest import BaseTestCase
from rdc.etl.hash import Hash
from rdc.etl.transform.map import Map


class TransformMapTestCase(BaseTestCase):
    def test_base_class_decorator(self):
        @Map
        def my_map(s):
            for l in s.split('\n'):
                yield Hash((('f%d' % (i, ), v) for i, v in enumerate(l.split(':'))))

        self.assertStreamEqual(
            my_map((Hash({'_': 'a:b:c\nb:c:d\nc:d:e'}))), (
                OrderedDict((('f0', 'a'), ('f1', 'b'), ('f2', 'c'), ), ),
                OrderedDict((('f0', 'b'), ('f1', 'c'), ('f2', 'd'), ), ),
                OrderedDict((('f0', 'c'), ('f1', 'd'), ('f2', 'e'), ), ),
            ))


if __name__ == '__main__':
    unittest.main()
