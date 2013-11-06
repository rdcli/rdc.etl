# -*- coding: utf-8 -*-
#
# Copyright 2012-2013 Romain Dorgueil
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

from rdc.etl.io.reader import Reader, Readable


class MyReadable(Readable):
    def __init__(self, uri):
        super(MyReadable, self).__init__()
        self._uri = uri

    def read(self):
        return 'read the ' + self._uri


class MyReader(Reader):
    readable = MyReadable


class TestIO(unittest.TestCase):
    def test_reader(self):
        reader = MyReader()
        with reader.open('foo') as r:
            value = r.read()
        self.assertEqual(value, 'read the foo')


if __name__ == '__main__':
    unittest.main()
