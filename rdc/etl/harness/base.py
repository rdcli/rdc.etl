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

from abc import ABCMeta, abstractmethod
from rdc.etl.error import AbstractError

class IHarness:
    """
    ETL harness interface.

    The harness is basically the executable stuff that will actually run a job.

    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def __call__(self):
        """Actual harness run."""
        raise AbstractError(self.__call__)

    @abstractmethod
    def add(self, transform):
        raise AbstractError(self.add)

    @abstractmethod
    def loop(self):
        raise AbstractError(self.loop)


class BaseHarness(IHarness):
    def initialize(self):
        pass

    def validate(self):
        pass

    def finalize(self):
        pass

    def __call__(self):
        """Implements IHarness.__call__()"""
        self.initialize()
        self.validate()
        _value = self.loop()
        self.finalize()
        return _value