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
#
from copy import copy
from pprint import pformat

class Hash(object):
    """
    Enhanced dictionary type used to communicate named tuples between transform blocks.

    @todo change this to be more standard python

    """

    def __init__(self, datadict=None):
        if datadict is not None:
            try:
                items = datadict.iteritems()
            except AttributeError, e:
                items = datadict

            for k, v in items:
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
        self.__dict__.update(dct.items())
        return self

    def get(self, key, default=None):
        """todo: decide which method is the right one for api, simplify"""
        try:
            if default is not None: 
                return getattr(self, key, default)
            else:
                return getattr(self, key)
        except AttributeError, e:
            if default is not None:
                return default
            raise KeyError('No value found for key %r.' % (key, ))

    def __getitem__(self, key):
        return self.get(key)

    def set(self, key, value):
        setattr(self, key, value)
        return self

    def __setitem__(self, key, value):
        return self.set(key, value)

    def has(self, key, allow_none=False):
        if hasattr(self, key):
            if allow_none:
                return True
            return self.get(key) is not None
        return False

    def __contains__(self, item):
        return self.has(item, True)

    def restrict(self, tester=None, renamer=None):
        """
        todo: simplify this as it does two things
        """
        for k, v in self.__dict__.items():
            if tester and not tester(k):
                delattr(self, k)
            elif renamer:
                setattr(self, renamer(k), v)
                delattr(self, k)

        return self

    def remove(self, *keys):
        for key in keys:
            if key in self.__dict__:
                self.__dict__.pop(key)
        return self

    def get_values(self, keys):
        return [self.get(key) for key in keys]

    def __repr__(self):
        return '<' + self.__class__.__name__ + ' ' + pformat(self.__dict__) + '>'

