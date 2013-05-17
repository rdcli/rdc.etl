# -*- coding: utf-8 -*-
#
# Author: Romain Dorgueil <romain@dorgueil.net>
# Copyright: © 2011-2013 SARL Romain Dorgueil Conseil
#

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
