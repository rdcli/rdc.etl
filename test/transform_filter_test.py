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
from rdc.etl.transform.filter import Filter

INPUT_DATA = (
    OrderedDict((('foo', 'bar'), ('keepme', True), )),
    OrderedDict((('foo', 'baz'), ('keepme', False), )),
)

class TransformFilterTestCase(BaseTestCase):
    def test_base_class_decorator(self):
        @Filter
        def my_filter(hash, channel):
            return hash['keepme'] == True

        out = my_filter(*INPUT_DATA)

        self.assertStreamEqual(out, (INPUT_DATA[0], ))


if __name__ == '__main__':
    unittest.main()
