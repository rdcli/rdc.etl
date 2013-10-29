# -*- coding: utf-8 -*-
#
# Author: Romain Dorgueil <romain@dorgueil.net>
# Copyright: © 2011-2013 SARL Romain Dorgueil Conseil
#
import sys
from rdc.etl.transform import Transform

class Log(Transform):
    field_filter = None
    prefix  = 'log> '
    prefix2 = '   > '

    def __init__(self, field_filter=None):
        super(Log, self).__init__()
        self.field_filter = field_filter or self.field_filter

    def format(self, s):
        s = s.split('\n')
        if not len(s[0].strip()):
            return ''

        s[0] = self.prefix + s[0]

        if len(s) > 1:
            s[1:] = [self.prefix2 + line for line in s[1:]]

        return '\n'.join(s)

    def output(self, s):
        print self.format(s)

    def transform(self, hash):
        self.output(repr(hash if not callable(self.field_filter) else hash.copy().restrict(self.field_filter)))
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

class Override(Transform):
    override_data = {}

    def __init__(self, override_data = None):
        super(Override, self).__init__()
        self.override_data = override_data or self.override_data

    def transform(self, hash):
        yield hash.update(self.override_data)
