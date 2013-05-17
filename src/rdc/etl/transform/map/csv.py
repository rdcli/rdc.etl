# -*- coding: utf-8 -*-
#
# Author: Romain Dorgueil <romain@dorgueil.net>
# Copyright: © 2011-2013 SARL Romain Dorgueil Conseil
#
from __future__ import absolute_import
import csv
from rdc.etl.transform import Transform
try:
    import cStringIO as StringIO
except:
    import StringIO


class CsvMap(Transform):
    field = None
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

    def transform(self, hash):
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

            yield hash.copy({self.field: row}).update({headers[i]: row[i] for i in range(0, field_count)})

