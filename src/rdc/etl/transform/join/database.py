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

from rdc.etl.transform.join import Join


class DatabaseJoin(Join):
    """
    Operates a simple cartesian product between a hash and a row collection coming from a dynamic database query.

    """
    query = None
    dataset_keys_for_values = []

    def __init__(self, engine, query=None, dataset_keys_for_values=None, is_outer=False, default_outer_join_data=None):
        super(DatabaseJoin, self).__init__(is_outer, default_outer_join_data)

        self.engine = engine
        self.query = query or self.query
        self.dataset_keys_for_values = dataset_keys_for_values or self.dataset_keys_for_values

    def get_join_data_for(self, hash):
        values = [hash.get(key) for key in self.dataset_keys_for_values]
        return self.engine.execute(self.query, values)

