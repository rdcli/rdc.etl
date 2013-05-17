# -*- coding: utf-8 -*-
#
# Author: Romain Dorgueil <romain@dorgueil.net>
# Copyright: © 2011-2013 SARL Romain Dorgueil Conseil
#
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

    def transform(self, hash):
        # this is a bit counterproductive, should tell that we don't change the flux, or delegate this to databaseload
        # or something
        yield hash






