# -*- coding: utf-8 -*-
#
# Author: Romain Dorgueil <romain@dorgueil.net>
# Copyright: © 2011-2013 SARL Romain Dorgueil Conseil
#
from copy import copy
from pprint import pformat


class Hash(object):
    def __init__(self, datadict=None):
        if datadict is not None:
            for k, v in datadict.items():
                k = k.replace('.', '_')
                setattr(self, k, v)

    def copy(self, datadict=None):
        o = copy(self)
        if datadict is not None:
            for k, v in datadict.items():
                o.set(k.replace('.', '_'), v)
        return o

    def items(self):
        return self.__dict__.items()

    def keys(self):
        return self.__dict__.keys()

    def values(self):
        return self.__dict__.values()

    def update(self, dct):
        self.__dict__.update(dct)
        return self

    def get(self, key, default=None):
        return getattr(self, key, default)

    def set(self, key, value):
        setattr(self, key, value)
        return self

    def has(self, key, allow_none=False):
        if hasattr(self, key):
            if allow_none:
                return True
            return self.get(key) is not None
        return False

    def __repr__(self):
        return '<' + self.__class__.__name__ + ' ' + pformat(self.__dict__) + '>'

    def restrict(self, tester, renamer=None):
        for k, v in self.__dict__.items():
            if not tester(k):
                delattr(self, k)
            elif renamer:
                setattr(self, renamer(k), v)
                delattr(self, k)
        return self

    def remove(self, key):
        delattr(self, key)
        return self

