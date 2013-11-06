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

from rdc.etl.io import STDIN
from rdc.etl.transform import Transform


class DatabaseCreateTable(Transform):
    table_name = None
    structure = ()
    drop_if_exists = False

    def __init__(self, engine, table_name=None, structure=None, drop_if_exists=None):
        super(DatabaseCreateTable, self).__init__()

        self.engine = engine
        self.table_name = table_name or self.table_name
        self.structure = structure or self.structure
        self.drop_if_exists = drop_if_exists or self.drop_if_exists
        self._executed = False

    def __call__(self, hash):
        if not self._executed:
            if self.drop_if_exists:
                query = 'DROP TABLE IF EXISTS %s;' % (self.table_name, )
                self.engine.execute(query)

            query = 'CREATE TABLE %s (%s)' % (
                self.table_name,
                ', \n'.join(['%s %s' % (n, t) for n, t in self.structure])
            )
            self.engine.execute(query)
            self._executed = True

        return super(DatabaseCreateTable, self).__call__(hash)

    def transform(self, hash, channel=STDIN):
        # this is a bit counterproductive, should tell that we don't change the flux, or delegate this to databaseload
        # or something
        yield hash

