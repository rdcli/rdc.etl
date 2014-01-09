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
from rdc.etl.extra.unittest import BaseTestCase
from rdc.etl.hash import Hash
from rdc.etl.transform.extract import Extract

INPUT_DATA = (
    {'foo': 'bar'},
    {'foo': 'baz'},
)

class TransformExtractTestCase(BaseTestCase):
    def test_base_class(self):
        t = Extract(extract=INPUT_DATA)
        self.assertStreamEqual(t(Hash()), INPUT_DATA)

    def test_base_class_generator(self):
        def generator():
            yield INPUT_DATA[0]
            yield INPUT_DATA[1]
        t = Extract(extract=generator)
        self.assertStreamEqual(t(Hash()), INPUT_DATA)

    def test_base_class_decorator(self):
        @Extract
        def my_iterable():
            return INPUT_DATA
        self.assertEqual(my_iterable.__name__, 'my_iterable')
        self.assertStreamEqual(my_iterable(Hash()), INPUT_DATA)

    def test_base_class_decorator_generator(self):
        @Extract
        def my_generator():
            yield INPUT_DATA[0]
            yield INPUT_DATA[1]
        self.assertEqual(my_generator.__name__, 'my_generator')
        self.assertStreamEqual(my_generator(Hash()), INPUT_DATA)

if __name__ == '__main__':
    unittest.main()
