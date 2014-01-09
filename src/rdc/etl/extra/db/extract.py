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

from rdc.etl.transform.extract import Extract


class DatabaseExtract(Extract):
    """
    Extract data from a database using some raw SQL and yield one output line per query result.

    .. attribute:: engine

        The sqlalchemy engine to use for extraction.

    .. attribute:: query

        The database query that will be used to extract data from database. Should not contain OFFSET/LIMIT, nor ";".

    .. attribute:: pack_size

        The number of records to retrieve at a time (will be used to add OFFSET/LIMIT clauses to SQL).

    """

    query = 'SELECT 1'
    pack_size = 1000

    def __init__(self, engine, query=None):
        super(DatabaseExtract, self).__init__()

        self.engine = engine
        self.query = query or self.query

    def extract(self):
        self.query = self.query.strip()
        if self.query[-1] == ';':
            self.query = self.query[0:-1]
        offset = 0

        while True:
            query = self.query + ' LIMIT ' + str(self.pack_size) + ' OFFSET ' + str(offset * self.pack_size) + ';'
            results = self.engine.execute(query, use_labels=True).fetchall()
            if not len(results):
                break

            for row in results:
                yield row

            offset += 1

