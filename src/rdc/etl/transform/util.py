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

import sys
from rdc.etl.transform import Transform


class Log(Transform):
    """Identity transform that adds a console output side effect, to watch what is going through Queues at some point
    of an ETL process.

    """

    field_filter = None
    condition = None
    prefix = 'log> '
    prefix2 = '   > '

    def __init__(self, field_filter=None, condition=None):
        """Initializes the Log transform. Usually, no

        :param field_filter:
            An optional callable that will be used to filter which keys to display on transform.
        :param condition:
            An optional callable that will be used to determine wether or not this row should be logged (aka sent to
            stdout).
        """
        super(Log, self).__init__()

        self.field_filter = field_filter or self.field_filter
        self.condition = condition or self.condition

    def format(self, s):
        """Row formater."""
        s = s.split('\n')
        if not len(s[0].strip()):
            return ''

        s[0] = self.prefix + s[0]

        if len(s) > 1:
            s[1:] = [self.prefix2 + line for line in s[1:]]

        return '\n'.join(s)

    def output(self, s):
        """Output method."""
        print self.format(s)

    def transform(self, hash):
        """Actual transformation."""
        if not self.condition or self.condition(hash):
            self.output(repr(hash if not callable(self.field_filter) else hash.copy().restrict(self.field_filter)))
        yield hash


class Stop(Transform):
    """Sinker transform that stops anything through the pipes.

    """

    def transform(self, hash):
        pass


class Halt(Transform):
    """Halt transform that exit process after a given number of skipped results. This may not have the effect you're
    thinking, as the skipped hashes will still go through future transformations, and using sys.exit is probably a very
    bad idea in the middle of a multithreaded process.

    DON'T USE THIS, OR PREPARE TO SUFFER SEVERE HEADACHES.

    @deprecated
    @todo remove this shit

    """
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
    """
    Simple transform that will overwrite some values with constant values provided in a Hash.

    """

    override_data = {}

    def __init__(self, override_data=None):
        super(Override, self).__init__()
        self.override_data = override_data or self.override_data

    def transform(self, hash):
        yield hash.update(self.override_data)
