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
from sqlalchemy import MetaData, Table
from werkzeug.utils import cached_property
from rdc.etl.transform import Transform
from rdc.etl.util import now


class BaseDatabaseLoad(Transform):
    table_name = None
    fetch_columns = None
    insert_only_fields = None
    discriminant = ('id', )
    created_at_field = 'created_at'
    updated_at_field = 'updated_at'

    def __init__(self, engine, table_name=None, fetch_columns=None, discriminant=None, created_at_field=None, updated_at_field=None, insert_only_fields=None):
        super(BaseDatabaseLoad, self).__init__()
        self.engine = engine
        self._query_count = 0
        self.table_name = table_name or self.table_name

        self.fetch_columns = {}
        if isinstance(fetch_columns, list): self.add_fetch_column(*fetch_columns)
        elif isinstance(fetch_columns, dict): self.add_fetch_column(**fetch_columns)

        self.discriminant = discriminant or self.discriminant
        self.created_at_field = created_at_field or 'created_at'
        self.updated_at_field = updated_at_field or 'updated_at'

        self.insert_only_fields = insert_only_fields or ()

    def get_existing_keys(self, dataset, insert=False):
        keys = dataset.keys()
        column_names = self.table.columns.keys()
        return [key for key in keys if key in column_names and (insert or (key not in self.insert_only_fields))]

    @cached_property
    def metadata(self):
        return MetaData()

    @cached_property
    def table(self):
        return Table(self.table_name, self.metadata, autoload=True, autoload_with=self.engine)

    def find(self, dataset, connection=None):
        query = 'SELECT * FROM ' + self.table_name + ' WHERE ' + (' AND '.join([key_atom + ' = %s' for key_atom in self.discriminant])) + ' LIMIT 1;'
        rp = (connection or self.connection).execute(query, [dataset.get(key_atom) for key_atom in self.discriminant])

        return rp.fetchone()

    def add_fetch_column(self, *columns, **aliased_columns):
        self.fetch_columns.update(aliased_columns)
        for column in columns:
            self.fetch_columns[column] = column

    @property
    def now(self):
        return now()

    def do_transform(self, hash):
        row = self.find(hash)

        now = self.now
        column_names = self.table.columns.keys()
        if self.updated_at_field in column_names:
            hash.set(self.updated_at_field, now)
        else:
            if hash.has(self.updated_at_field):
                hash.remove(self.updated_at_field)

        if row:
            dataset_keys = self.get_existing_keys(hash, insert=False)
            query = 'UPDATE ' + self.table_name + ' SET ' + ', '.join(['%s = %%s' % (col, ) for col in dataset_keys if not col in self.discriminant]) + ' WHERE ' + (' AND '.join([key_atom + ' = %s' for key_atom in self.discriminant]))
            values = [hash.get(col) for col in dataset_keys if not col in self.discriminant] + [hash.get(col) for col in self.discriminant]
        else:
            if self.created_at_field in column_names:
                hash.set(self.created_at_field, now)
            else:
                if hash.has(self.created_at_field):
                    hash.remove(self.created_at_field)
            dataset_keys = self.get_existing_keys(hash, insert=True)
            query = 'INSERT INTO ' + self.table_name + ' (' + ', '.join(dataset_keys) + ') VALUES (' + ', '.join(['%s' for col in dataset_keys]) + ')'
            values = [hash.get(key) for key in dataset_keys]

        self.connection.execute(query, values)

        if self.fetch_columns and len(self.fetch_columns):
            if not row:
                row = self.find(hash)
            if not row:
                raise ValueError('Could not find matching row after load.')

            for alias, column in self.fetch_columns.items():
                hash.set(alias, row[column])

        return hash

    def transform(self, hash, channel=STDIN):
        with self.connection.begin():
            yield self.do_transform(hash)

class DatabaseLoad(BaseDatabaseLoad):
    buffer_size = 1000

    def __init__(self, engine, table_name=None, fetch_columns=None, discriminant=None, created_at_field=None,
                 updated_at_field=None, insert_only_fields=None):
        super(DatabaseLoad, self).__init__(engine, table_name, fetch_columns, discriminant, created_at_field,
                                            updated_at_field, insert_only_fields)

        self.buffer = []
        self._connection = None

    @property
    def connection(self):
        if self._connection is None:
            self._connection = self.engine.connect()
        return self._connection

    def close_connection(self):
        self._connection.close()
        self._connection = None

    def commit(self):
        with self.connection.begin():
            while len(self.buffer):
                hash = self.buffer.pop(0)
                yield self.do_transform(hash)

    def transform(self, hash, channel=STDIN):
        self.buffer.append(hash)

        if len(self.buffer) >= self.buffer_size:
            for _out in self.commit():
                yield _out

    def finalize(self):
        for _out in self.commit():
            yield _out

        self.close_connection()

