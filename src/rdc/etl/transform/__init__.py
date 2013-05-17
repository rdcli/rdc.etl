# -*- coding: utf-8 -*-
#
# Author: Romain Dorgueil <romain@dorgueil.net>
# Copyright: © 2011-2013 SARL Romain Dorgueil Conseil
#
import types


class Transform(object):
    def __init__(self):
        self._s_in = 0
        self._s_out = 0

    @abstract
    def transform(self, hash): pass

    def __call__(self, hash):
        self._s_in += 1

        t = self.transform(hash)

        if isinstance(t, types.GeneratorType):
            for _out in t:
                self._s_out += 1
                yield _out
        elif t is not None:
            self._s_out += 1
            yield t

    def __repr__(self):
        return '<'+self.__class__.__name__+' in='+str(self._s_in)+' out='+str(self._s_out)+'>'