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
from copy import copy

import sys
from rdc.etl.error import AbstractError
from rdc.etl.hash import Hash
from rdc.etl.io import STDIN, STDOUT, STDIN2, STDOUT2
from rdc.etl.transform import Transform
from rdc.etl.util import terminal as t

def shade(v):
    return t.black(t.bold(v))

class Log(Transform):
    """Identity transform that adds a console output side effect, to watch what is going through Queues at some point
    of an ETL process.

    """

    field_filter = None
    condition = None
    clean = False

    def __init__(self, field_filter=None, condition=None, clean=None):
        """Initializes the Log transform. Usually, no

        :param field_filter:
            An optional callable that will be used to filter which keys to display on transform.
        :param condition:
            An optional callable that will be used to determine whether or not this row should be logged (aka sent to
            stdout).
        """
        super(Log, self).__init__()

        self.field_filter = field_filter or self.field_filter
        self.condition = condition or self.condition
        self.clean = clean or self.clean

    def format(self, s):
        """Row formater."""

        # pretty format Hashes
        if isinstance(s, Hash):
            _s, s = s, []
            for k in _s.keys():
                s.append(u'  {k}{t.black}:{t.bold}{tp}{t.normal} {t.black}{t.bold}→{t.normal} {t.black}«{t.normal}{v}{t.black}»{t.normal}{t.clear_eol}'.format(k=k, v=_s[k], t=t, tp=type(_s[k]).__name__))
        else:
            # unpack
            s = s.split('\n')

        # hack if one empty line
        if len(s) < 2 and not len(s[0].strip()):
            return ''

        # pack it back
        return '\n'.join(s)


    def writehr(self, label=None):
        if label:
            label = unicode(label)
            sys.stderr.write(t.black(u'·' * 4) + shade('{') + label + shade('}') + t.black(u'·' * (t.width - (6+len(label)) - 1)) + '\n')
        else:
            sys.stderr.write(t.black(u'·' * (t.width-1) + '\n'))


    def writeln(self, s):
        """Output method."""
        sys.stderr.write(self.format(s) + '\n')

    def initialize(self):
        self.lineno = 0

    def transform(self, hash, channel=STDIN):
        """Actual transformation."""
        self.lineno += 1
        if not self.condition or self.condition(hash):
            hash = hash.copy()
            hash = hash if not callable(self.field_filter) else hash.restrict(self.field_filter)
            if self.clean:
                hash = hash.restrict(lambda k: len(k) and k[0] != '_')
            self.writehr(self.lineno)
            self.writeln(hash)
            self.writehr()
            sys.stderr.write('\n')
        yield hash

class Limit(Transform):
    """Only pass the first `limit` input lines to default output.

    Args:
      limit (int): Number of line after which to stop passing input lines to the output.

    """

    limit = 0

    def __init__(self, limit = None):
        super(Limit, self).__init__()

        self.limit = limit or self.limit

    def initialize(self):
        super(Limit, self).initialize()
        self._current = 0

    def transform(self, hash, channel=STDIN):
        self._current += 1

        if self.limit and self._current <= self.limit:
            yield hash

class Stop(Transform):
    """Sinker transform that stops anything through the pipes.

    """

    def transform(self, hash, channel=STDIN):
        pass


class Override(Transform):
    """
    Simple transform that will overwrite some values with constant values provided in a Hash.

    """

    override_data = {}

    def __init__(self, override_data=None):
        super(Override, self).__init__()
        self.override_data = override_data or self.override_data

    def transform(self, hash, channel=STDIN):
        yield hash.update(self.override_data)

def clean(hash):
    for k in hash:
        if k.startswith('_'):
            del hash[k]
    return hash

class Clean(Transform):
    """
    Remove all fields with keys starting by _
    """

    def transform(self, hash, channel=STDIN):
        yield clean(hash)


class Validate(Transform):
    """An identity transform (input is passed to output unchanged). Aims for data validation."""

    CHANNEL_MAP = {
        STDIN: STDOUT,
        STDIN2: STDOUT2,
    }

    def __init__(self, validate=None):
        # Use the callable name if provided
        if validate and not self._name:
            self._name = validate.__name__

        # Parent constructor
        super(Validate, self).__init__()

        self.validate = validate or self.validate


    def validate(self, hash, channel=STDIN):
        raise AbstractError(self.validate)

    def transform(self, hash, channel=STDIN):
        # TODO instead of a copy, pass a "ReadOnly" decorated hash.
        self.validate(copy(hash), channel)
        yield hash, self.CHANNEL_MAP[STDIN]