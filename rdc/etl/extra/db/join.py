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

from collections import OrderedDict
import itertools

from rdc.etl.error import AbstractError
from rdc.etl.io import STDIN
from rdc.etl.transform.join import Join


class DatabaseJoin(Join):
    """
    Operates a simple cartesian product between a hash and a row collection coming from a dynamic database query.

    For now, you need to pass an sqlite engine to contructor, as the work on external services connections won't be in
    milestone 1.0.

    .. attribute:: query

        SQL query template used to join flux data with database relation.

    .. attribute:: dataset_keys_for_values

        List of hash keys used to populate %-values in sql. For example, if query == "SELECT name FROM user WHERE id = %s",
        this tuple can be ('id', ), meaning that for each input line, the "id" field value will be used to execute
        the statement. Each sql statement output row will be joined with the input row.

    """

    query = None
    dataset_keys_for_values = []

    def __init__(self, engine, query=None, dataset_keys_for_values=None, is_outer=False, default_outer_join_data=None):
        super(DatabaseJoin, self).__init__(is_outer=is_outer, default_outer_join_data=default_outer_join_data)

        # parameters
        self.engine = engine
        self.query = query or self.query
        self.dataset_keys_for_values = dataset_keys_for_values or self.dataset_keys_for_values

        # database connection
        self._connection = None

    def join(self, hash, channel=STDIN):
        """Get data to join with from database."""
        return self.connection.execute(self.query, [
            hash[key] for key in self.dataset_keys_for_values
        ])

    def finalize(self):
        """
        Finalize the transformation.

        DBAPI connection should be cleaned up.

        """
        super(DatabaseJoin, self).finalize()

        self._close_connection()

    @property
    def connection(self):
        """
        Database connection. If not done yet, connect().

        TODO: check what happens if for some reason the connection has been closed but attribute was not set to None.

        """
        if not self._connection:
            self._connection = self.engine.connect()
        return self._connection

    def _close_connection(self):
        """
        Closes database connection.

        """
        if self._connection:
            self._connection.close()
        self._connection = None


class DatabaseJoinOrCreate(Join):
    """
    Find a link in a related table, or create related object if it cannot be found.

    Relies on three concepts: identity, params and output.

    Identity is the dictionary that will be used for the "find" criteria.
    Params is the dictionary that will be used to populate new database row values.
    Output takes the found mapped item in database, and returns a dictionary of the hash fields to update.

    Values are cached, so you cannot yield different modifications from the same object. For more informations, RTFS.

    """

    table_name = None

    def __init__(self, engine, table_name = None, identity = None, params = None, output = None):
        super(DatabaseJoinOrCreate, self).__init__()
        self.engine = engine
        self.table_name = table_name or self.table_name
        self.identity = callable(identity) and identity or self.identity
        self.params = callable(params) and params or self.params
        self.output = callable(output) and output or self.output

        self._result_cache = {}

    def identity(self, hash):
        raise AbstractError(self.identity)

    def params(self, hash):
        return {}

    def output(self, mapped):
        raise AbstractError(self.output)

    @classmethod
    def get_cache_key(cls, identity):
        return hash(tuple(sorted(identity.items())))

    def get_find_sql(self, identity):
        """Get SQL for object retrieval.

        :param identity:
        :return:
        """
        return '''
            SELECT *
            FROM {table_name} t
            WHERE {where}
            LIMIT 1
        '''.strip().format(
            table_name = self.table_name,
            where = ' AND '.join(('t.{field} = %s'.format(field=field) for field, value in identity.items()))
        )

    def get_create_sql(self, params):
        """Get SQL for object creation.

        :param identity:
        :param params:
        :return:
        """
        return '''
            INSERT INTO {table_name}
            ({fields}) VALUES ({values})
        '''.strip().format(
            table_name = self.table_name,
            fields = ', '.join((field for field in params.keys())),
            values = ', '.join(['%s'] * len(params)),
            )

    def find(self, identity):
        """Find an object based on identity.

        :param identity:
        :return:
        """
        return self.engine.execute(
            self.get_find_sql(identity),
            *identity.values()
        ).fetchone()

    def create(self, identity, params):
        """Create an object based on identity and params.

        :param identity:
        :param params:
        :return:
        """
        params = OrderedDict(itertools.chain(params.iteritems(), identity.iteritems()))
        self.engine.execute(
            self.get_create_sql(params),
            *params.values()
        )
        return self.find(identity)

    def join(self, hash, channel=STDIN):
        if channel != STDIN:
            raise ValueError('Unsupported channel')

        identity = self.identity(hash)
        assert len(identity), 'Identity should not be empty, how the fuck do you want me to retrieve an object otherwise?'

        _key = self.get_cache_key(identity)

        if not _key in self._result_cache:
            try:
                mapped = self.find(identity)
                if not mapped:
                    mapped = self.create(identity, self.params(hash))
                if not mapped:
                    raise RuntimeError('Could not find or create associated object, aborting.')
                self._result_cache[_key] = self.output(mapped)
            except:
                self._result_cache[_key] = False
                raise

        if self._result_cache[_key]:
            yield hash.copy(self._result_cache[_key])

