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
# -*- coding: utf-8 -*-

from __future__ import absolute_import
import csv
from rdc.etl.io import STDIN
from rdc.etl.transform import Transform

try:
    import cStringIO as StringIO
except:
    import StringIO


class CsvMap(Transform):
    field = '_'
    delimiter = ';'
    quotechar = '"'
    headers = None
    skip = 0

    def __init__(self, field=None, delimiter=None, quotechar=None, headers=None, skip=None):
        super(CsvMap, self).__init__()

        self.field = field or self.field
        self.delimiter = delimiter or self.delimiter
        self.quotechar = quotechar or self.quotechar
        self.headers = headers or self.headers
        self.skip = skip or self.skip

    @property
    def has_headers(self):
        return bool(self.headers)

    def transform(self, hash, channel=STDIN):
        s_in = StringIO.StringIO(hash.get(self.field))
        reader = csv.reader(s_in, delimiter=self.delimiter, quotechar=self.quotechar)
        headers = self.has_headers and self.headers or reader.next()
        field_count = len(headers)

        if self.skip and self.skip > 0:
            for i in range(0, self.skip):
                reader.next()

        for row in reader:
            if len(row) != field_count:
                raise ValueError('Got a line with %d fields, expecting %d.' % (len(row), field_count, ))

            yield hash.copy({self.field: row}).update(dict([(headers[i], row[i]) for i in range(0, field_count)]))

