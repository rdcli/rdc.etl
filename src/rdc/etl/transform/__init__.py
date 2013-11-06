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

import types


class Transform(object):
    def __init__(self):
        self._s_in = 0
        self._s_out = 0

    @abstract
    def transform(self, hash):
        pass

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

    def initialize(self):
        pass

    def finalize(self):
        pass

    def __repr__(self):
        return '<' + self.__class__.__name__ + ' in=' + str(self._s_in) + ' out=' + str(self._s_out) + '>'