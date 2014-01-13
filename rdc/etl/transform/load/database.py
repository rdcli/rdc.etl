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

from sqlalchemy import MetaData, Table
from rdc.etl.io import STDIN, STDOUT, INSERT, UPDATE, SELECT
from rdc.etl.transform import Transform
from rdc.etl.util import now, cached_property


class DatabaseLoad(Transform):
    """

    TODO doc this !!! test this !!!!

    """

    engine = None
    table_name = None
    fetch_columns = None
    insert_only_fields = ()
    discriminant = ('id', )
    created_at_field = 'created_at'
    updated_at_field = 'updated_at'

    def __init__(self, engine=None, table_name=None, fetch_columns=None, discriminant=None, created_at_field=None,
                 updated_at_field=None, insert_only_fields=None):
        super(DatabaseLoad, self).__init__()

        self.engine = engine or self.engine
        self.table_name = table_name or self.table_name

        # xxx should take self.fetch_columns into account if provided
        self.fetch_columns = {}
        if isinstance(fetch_columns, list):
            self.add_fetch_column(*fetch_columns)
        elif isinstance(fetch_columns, dict):
            self.add_fetch_column(**fetch_columns)

        self.discriminant = discriminant or self.discriminant
        self.created_at_field = created_at_field or self.created_at_field
        self.updated_at_field = updated_at_field or self.updated_at_field
        self.insert_only_fields = insert_only_fields or self.insert_only_fields

        self._buffer = []
        self._connection = None
        self._max_buffer_size = 1000
        self._last_duration = None
        self._last_commit_at = None
        self._query_count = 0

    @property
    def connection(self):
        if self._connection is None:
            self._connection = self.engine.connect()
        return self._connection

    def commit(self):
        with self.connection.begin():
            while len(self._buffer):
                hash = self._buffer.pop(0)
                yield self.do_transform(hash)

    def close_connection(self):
        self._connection.close()
        self._connection = None

    def get_insert_columns_for(self, hash):
        """List of columns we can use for insert."""
        return self.columns

    def get_update_columns_for(self, hash, row):
        """List of columns we can use for update."""
        return [
            column for column in self.columns
            if not column in self.insert_only_fields
        ]

    def get_columns_for(self, hash, row=None):
        """Retrieve list of table column names for which we have a value in given hash.

        """
        if row:
            column_names = self.get_update_columns_for(hash, row)
        else:
            column_names = self.get_insert_columns_for(hash)

        return [key for key in hash if key in column_names]

    def find(self, dataset, connection=None):
        query = '''SELECT * FROM {table} WHERE {criteria} LIMIT 1'''.format(
            table=self.table_name,
            criteria=' AND '.join([key_atom + ' = %s' for key_atom in self.discriminant]),
        )
        rp = (connection or self.connection).execute(query, [dataset.get(key_atom) for key_atom in self.discriminant])

        # Increment stats
        self._input._special_stats[SELECT] += 1

        return rp.fetchone()

    def initialize(self):
        super(DatabaseLoad, self).initialize()

        self._input._special_stats[SELECT] = 0
        self._output._special_stats[INSERT] = 0
        self._output._special_stats[UPDATE] = 0


    def do_transform(self, hash):
        """Actual database load transformation logic, without the buffering / transaction logic.

        """

        # find line, if it exist
        row = self.find(hash)

        now = self.now
        column_names = self.table.columns.keys()
        # UpdatedAt field configured ? Let's set the value in source hash
        if self.updated_at_field in column_names:
            hash[self.updated_at_field] = now
        # Otherwise, make sure there is no such field
        else:
            if self.updated_at_field in hash:
                del hash[self.updated_at_field]

        # UPDATE
        if row:
            _columns = self.get_columns_for(hash, row)

            query = '''UPDATE {table} SET {values} WHERE {criteria}'''.format(
                table=self.table_name,
                values=', '.join((
                    '{column} = %s'.format(column=_column) for _column in _columns
                    if not _column in self.discriminant
                )),
                criteria=' AND '.join((
                    '{key} = %s'.format(key=_key) for _key in self.discriminant
                ))
            )
            values = [hash[_column] for _column in _columns if not _column in self.discriminant] + \
                     [hash[_column] for _column in self.discriminant]

        # INSERT
        else:
            if self.created_at_field in column_names:
                hash[self.created_at_field] = now
            else:
                if self.created_at_field in hash:
                    del hash[self.created_at_field]

            _columns = self.get_columns_for(hash)
            query = '''INSERT INTO {table} ({keys}) VALUES ({values})'''.format(
                table=self.table_name,
                keys=', '.join(_columns),
                values=', '.join(['%s'] * len(_columns))
            )
            values = [hash[key] for key in _columns]

        # Execute
        self.connection.execute(query, values)

        # Increment stats
        if row:
            self._output._special_stats[UPDATE] += 1
        else:
            self._output._special_stats[INSERT] += 1


        # If user required us to fetch some columns, let's query again to get their actual values.
        if self.fetch_columns and len(self.fetch_columns):
            if not row:
                row = self.find(hash)
            if not row:
                raise ValueError('Could not find matching row after load.')

            for alias, column in self.fetch_columns.iteritems():
                hash[alias] = row[column]

        return hash

    def transform(self, hash, channel=STDIN):
        """Transform method. Stores the input in a buffer, and only unstack buffer content if some limit has been
        exceeded.

        TODO for now buffer limit is hardcoded as 1000, but we may use a few criterias to add intelligence to this:
             time since last commit, duration of last commit, buffer length ...

        """
        self._buffer.append(hash)

        if len(self._buffer) >= self._max_buffer_size:
            for _out in self.commit():
                yield _out

    def finalize(self):
        """Transform's finalize method.

        Empties the remaining lines in buffer by loading them into database and close database connection.

        """

        super(DatabaseLoad, self).finalize()

        for _out in self.commit():
            yield _out

        self.close_connection()

    def add_fetch_column(self, *columns, **aliased_columns):
        self.fetch_columns.update(aliased_columns)
        for column in columns:
            self.fetch_columns[column] = column

    @cached_property
    def columns(self):
        return self.table.columns.keys()

    @cached_property
    def metadata(self):
        """SQLAlchemy metadata."""
        return MetaData()

    @cached_property
    def table(self):
        """SQLAlchemy table object, using metadata autoloading from database to avoid the need of column definitions."""
        return Table(self.table_name, self.metadata, autoload=True, autoload_with=self.engine)

    @property
    def now(self):
        """Current timestamp, used for created/updated at fields."""
        return now()

