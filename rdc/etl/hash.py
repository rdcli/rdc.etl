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
#
from collections import OrderedDict
from copy import copy

try:
    from thread import get_ident as _get_ident
except ImportError:
    from dummy_thread import get_ident as _get_ident

class Hash(OrderedDict):
    """
    Enhanced ordered dictionary type used to communicate named tuples between transform blocks.

    """

    def copy(self, datadict=None):
        o = copy(self)
        if datadict is not None:
            for k, v in datadict.items():
                o[k] = v
        return o

    def restrict(self, tester=None, renamer=None):
        """
        todo: simplify this as it does two things
        """
        keys = self.keys()
        for k in keys:
            if tester and not tester(k):
                del self[k]
            elif renamer:
                self[renamer(k)] = self[k]
                del self[k]
        return self

    def update(self, other=None, **kwargs):
        super(Hash, self).update(other, **kwargs)
        return self

    def remove(self, *keys):
        for key in keys:
            if key in self:
                del self[key]
        return self

    def get_values(self, keys):
        return [self[key] for key in keys]

    # BC
    def has(self, k, allow_none=False):
        return k in self and (allow_none or self[k] is not None)

    # BC
    def set(self, k, v):
        self[k] = v
        return self

    def __repr__(self, _repr_running={}):
        call_key = id(self), _get_ident()
        if call_key in _repr_running:
            return '...'
        _repr_running[call_key] = 1
        try:
            if not self:
                return 'H{}' % ()
            return 'H{%s}' % (', '.join(['%r: %r' % (k, v, ) for k, v in self.iteritems()]), )
        finally:
            del _repr_running[call_key]

