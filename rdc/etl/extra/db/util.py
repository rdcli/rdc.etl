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

from threading import Lock
from rdc.etl.transform import Transform

class DbTransform(Transform):
    """Base class for transformations needing a database engine.

    """

    def __init__(self, db):
        super(DbTransform, self).__init__()
        self.db = db

class DbEngineThreadsafeWrapper(object):
    """Avoid concurrency problems when using the same engine in multiple transforms running at the same time.
    TODO: make generic __getattr__ that returns a locked wrapper around arbitrary named methods.

    """

    def __init__(self, engine):
        self._engine = engine
        self._lock = Lock()

    def execute(self, *args, **kwargs):
        with self._lock:
            return self._engine.execute(*args, **kwargs)
