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
from rdc.etl.extra.simple import SimpleTransform


class TransformSimpleTestCase(unittest.TestCase):
    def _create_transform(self):
        return SimpleTransform()

    def test_remove(self):
        t = self._create_transform()
        t.remove('foo', 'boo')
        r = t.transform(Hash({'foo': 'bar', 'bar': 'baz', 'boo': 'hiya'}))
        self.assertIn('bar', r)
        self.assertNotIn('foo', r)
        self.assertNotIn('boo', r)

if __name__ == '__main__':
    unittest.main()
