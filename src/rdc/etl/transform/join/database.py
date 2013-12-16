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



