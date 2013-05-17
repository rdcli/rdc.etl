# -*- coding: utf-8 -*-
#
# Author: Romain Dorgueil <romain@dorgueil.net>
# Copyright: © 2011-2013 SARL Romain Dorgueil Conseil
#
import sys
from rdc.etl.transform import Transform

class Log(Transform):
    field_filter = None

    def __init__(self, field_filter=None):
        super(Log, self).__init__()
        self.field_filter = field_filter or self.field_filter

    def transform(self, hash):
        print '<log>', hash if not callable(self.field_filter) else hash.copy().restrict(self.field_filter)
        yield hash

class Stop(Transform):
    def transform(self, hash):
        pass

class Halt(Transform):
    skip = 0

    def __init__(self, skip=None):
        super(Halt, self).__init__()

        self.skip = skip or self.skip

    def transform(self, hash):
        if self.skip and self.skip > 0:
            self.skip -= 1
            yield hash

        print hash
        print '=== HALT ! ==='
        sys.exit(1)