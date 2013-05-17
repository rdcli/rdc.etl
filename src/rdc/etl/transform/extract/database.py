# -*- coding: utf-8 -*-
#
# Author: Romain Dorgueil <romain@dorgueil.net>
# Copyright: © 2011-2013 SARL Romain Dorgueil Conseil
#

from rdc.etl.transform.extract import Extract

class DatabaseExtract(Extract):
    query = 'SELECT 1'
    pack_size = 1000

    def __init__(self, engine):
        super(DatabaseExtract, self).__init__()

        self.engine = engine

    def transform(self, hash):
        self.query = self.query.strip()
        if self.query[-1] == ';':
            self.query = self.query[0:-1]

        offset = 0

        while True:
            query = self.query + ' LIMIT ' + str(self.pack_size) + ' OFFSET ' + str(offset*self.pack_size) + ';'
            results = self.engine.execute(query, use_labels=True).fetchall()
            if not len(results):
                break

            for row in results:
                yield hash.copy(row)

            offset += 1

