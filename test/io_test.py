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
from Queue import Empty
from rdc.etl.io import Input, InactiveWritableError, Begin, End, InactiveReadableError, InputMultiplexer


class InputTestCase(unittest.TestCase):
    def test_input(self):
        q = Input()

        # Before Begin, noone should be able to write in an Input queue.
        self.assertEqual(q.alive, False)
        self.assertRaises(InactiveWritableError, q.put, 'foo')

        # Begin
        q.put(Begin)
        self.assertEqual(q.alive, True)
        self.assertEqual(q._runlevel, 1)
        q.put('foo')

        # Second Begin
        q.put(Begin)
        self.assertEqual(q.alive, True)
        self.assertEqual(q._runlevel, 2)
        q.put('bar')
        q.put(End)

        # FIFO
        self.assertEqual(q.get(), 'foo')
        self.assertEqual(q.get(), 'bar')

        # self.assertEqual(q.alive, False) XXX queue don't know it's dead yet, but it is ...
        # Async get raises Empty (End is not returned)
        self.assertRaises(Empty, q.get, block=False)
        self.assertEqual(q.alive, True)

        # Before killing, let's slide some data in.
        q.put('baz')

        # now kill the queue, try to write
        q.put(End)
        self.assertRaises(InactiveWritableError, q.put, 'foo')

        # still can get remaining data
        self.assertEqual(q.get(), 'baz')
        self.assertRaises(InactiveReadableError, q.get)

CH1 = 'ch1'
CH2 = 'ch2'

class InputMultiplexerTestCase(unittest.TestCase):
    def test_multiple_input(self):
        imux = InputMultiplexer([CH1, CH2])

        # inactive on mux creation
        self.assertRaises(InactiveWritableError, imux[CH1].put, 'foo')
        self.assertRaises(InactiveWritableError, imux[CH2].put, 'foo')

        # activate
        imux[CH1].put(Begin)
        imux[CH1].put('foo')
        imux[CH1].put('foo-oo')

        # simple get.
        self.assertEqual(imux.get(), ('foo', CH1, ))

        imux[CH1].put(End)

        imux[CH2].put(Begin)
        imux[CH2].put('bar')
        imux[CH2].put('ba-ar')
        imux[CH2].put(End)

        self.assertEqual(imux.get(), ('foo-oo', CH1, ))
        self.assertEqual(imux.get(), ('bar', CH2, ))
        self.assertEqual(imux.get(), ('ba-ar', CH2, ))
        self.assertRaises(InactiveReadableError, imux.get)



if __name__ == '__main__':
    unittest.main()
