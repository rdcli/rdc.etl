# -*- coding: utf-8 -*-
#
# copyright 2012-2014 romain dorgueil
#
# licensed under the apache license, version 2.0 (the "license");
# you may not use this file except in compliance with the license.
# you may obtain a copy of the license at
#
#     http://www.apache.org/licenses/license-2.0
#
# unless required by applicable law or agreed to in writing, software
# distributed under the license is distributed on an "as is" basis,
# without warranties or conditions of any kind, either express or implied.
# see the license for the specific language governing permissions and
# limitations under the license.

from rdc.etl.error import AbstractError, ValidationError
from rdc.etl.transform import Transform


class SqlExec(Transform):
    def __init__(self, sql=None, db=None):
        # Use the callable name if provided
        if sql and not self._name:
            self._name = sql.__name__

        super(SqlExec, self).__init__()

        self.db = db
        self.sql = sql or self.sql


    def sql(self, hash, channel):
        raise AbstractError(self.sql)

    def validate(self):
        if self.db is None:
            raise ValidationError(self, '"db" positional argument is required.')

    def transform(self, hash, channel):
        try:
            params = self.sql(hash, channel)
            if params:
                sql, params = params[0], params[1:]
                self.db.execute(sql, params)
                hash['@{0}#{1}@status'.format(self.__name__, id(self))] = 'success'
        except Exception as e:
            hash['@{0}#{1}@status'.format(self.__name__, id(self))] = 'failure'
            raise
        finally:
            yield hash
