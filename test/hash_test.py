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


class HashTestCase(unittest.TestCase):
    def test_constructor_hash(self):
        hash = Hash({'foo': 'bar', 'bar': 'baz'})
        self.assertEqual(hash.get('foo'), 'bar')
        self.assertEqual(hash.get('bar'), 'baz')

    def test_constructor_zippedtuples(self):
        hash = Hash((('foo', 'bar', ), ('bar', 'baz', ), ))
        self.assertEqual(hash.get('foo'), 'bar')
        self.assertEqual(hash.get('bar'), 'baz')

    def test_constructor_default(self):
        hash = Hash()
        self.assertRaises(KeyError, hash.__getitem__, 'anything')

    def test_method_getter(self):
        hash = Hash({'foo': 'bar', 'bar': None})
        self.assertEqual(hash.get('foo', 'baz'), 'bar')
        self.assertEqual(hash.get('bar', 'baz'), None)
        self.assertEqual(hash.get('boo', 'foo'), 'foo')
        self.assertEqual(hash.get('boo'), None)

    def test_item_getter(self):
        hash = Hash({'foo': 'bar', 'bar': None})
        self.assertEqual(hash['foo'], 'bar')
        self.assertEqual(hash['bar'], None)
        self.assertRaises(KeyError, hash.__getitem__, 'boo')

    def test_method_setter(self):
        hash = Hash({'foo': 'bar'})
        hash.set('bar', 'heya')
        self.assertEqual(hash['bar'], 'heya')
        self.assertEqual(hash['foo'], 'bar')
        hash.set('foo', 'yoho')
        self.assertEqual(hash['foo'], 'yoho')
        self.assertEqual(hash.set('any', 'thing'), hash)

    def test_item_setter(self):
        hash = Hash({'foo': 'bar'})
        hash['bar'] = 'heya'
        self.assertEqual(hash['bar'], 'heya')
        self.assertEqual(hash['foo'], 'bar')
        hash['foo'] = 'yoho'
        self.assertEqual(hash['foo'], 'yoho')

    def test_method_in(self):
        hash = Hash({'foo': 'bar', 'bar': None})
        self.assertEqual(hash.has('foo'), True)
        self.assertEqual(hash.has('bar'), False)
        self.assertEqual(hash.has('bar', True), True)
        self.assertEqual(hash.has('baz'), False)

    def test_operator_in(self):
        hash = Hash({'foo': 'bar', 'bar': None})
        self.assertEqual('foo' in hash, True)
        self.assertEqual('bar' in hash, True)
        self.assertEqual('baz' in hash, False)

    def test_copy(self):
        h1 = Hash({'foo': 'bar', 'bar': 'baz', })
        h2 = h1.copy()
        h3 = h1.copy({'bar': 'oh my bar'})
        h1['foo'] = 'original foo'
        h2['foo'] = 'new foo'
        self.assertEqual(h1['foo'], 'original foo')
        self.assertEqual(h2['foo'], 'new foo')
        self.assertEqual(h3['foo'], 'bar')
        self.assertEqual(h3['bar'], 'oh my bar')

    def test_restrict(self):
        h = Hash({'foo': 'bar', 'bar': 'baz', })
        h.restrict(tester=lambda k: k in ('bar', ))
        self.assertTrue(not 'foo' in h)
        self.assertTrue('bar' in h)

    def test_restrict_with_renamer(self):
        h = Hash({'foo': 'bar', 'bar': 'baz', })
        h.restrict(tester=lambda k: k in ('bar', ), renamer=lambda k: k.upper())
        self.assertTrue(not 'foo' in h)
        self.assertTrue(not 'bar' in h)
        self.assertTrue('BAR' in h)
        self.assertEqual(h['BAR'], 'baz')

    def test_remove(self):
        h = Hash({'foo': 'bar', 'bar': 'baz', 'baz': 'boo', })
        h.remove('foo', 'baz')
        self.assertTrue(not 'foo' in h)
        self.assertTrue('bar' in h)
        self.assertTrue(not 'baz' in h)

    def test_get_values(self):
        h = Hash({'foo': 'bar', 'bar': 'baz', 'baz': 'boo', })
        self.assertEquals(h.get_values(('baz', 'foo', 'bar', )), ['boo', 'bar', 'baz', ])

if __name__ == '__main__':
    unittest.main()
