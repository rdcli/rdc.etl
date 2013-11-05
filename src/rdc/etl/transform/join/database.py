# -*- coding: utf-8 -*-
#
# Author: Romain Dorgueil <romain@dorgueil.net>
# Copyright: © 2011-2013 SARL Romain Dorgueil Conseil
#
from rdc.etl.transform.join import Join


class DatabaseJoin(Join):
    """
    Operates a simple cartesian product between a hash and a row collection coming from a dynamic database query.

    """
    query = None
    dataset_keys_for_values = []

    def __init__(self, engine, query=None, dataset_keys_for_values=None, is_outer=False, default_outer_join_data = None):
        super(DatabaseJoin, self).__init__(is_outer, default_outer_join_data)

        self.engine = engine
        self.query = query or self.query
        self.dataset_keys_for_values = dataset_keys_for_values or self.dataset_keys_for_values

    def get_join_data_for(self, hash):
        values = [hash.get(key) for key in self.dataset_keys_for_values]
        return self.engine.execute(self.query, values)

