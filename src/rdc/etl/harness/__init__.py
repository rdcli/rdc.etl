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

from abc import ABCMeta, abstractmethod

class IHarness:
    """
    ETL harness interface.

    The harness is basically the executable stuff that will actually run a job.

    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def __call__(self):
        """Actual harness run."""

    @abstractmethod
    def add(self, transform): pass

    @abstractmethod
    def loop(self): pass


class AbstractHarness(IHarness):
    """
    is 99.9% chances you want to extend this or a subclass of this.

    """

    __metaclass__ = ABCMeta

    def __init__(self):
        self.status = []

    def initialize(self): pass

    def validate(self): pass

    def finalize(self): pass

    def __call__(self):
        """Implements IHarness.__call__()"""
        self.initialize()
        self.validate()
        _value = self.loop()
        self.finalize()
        return _value

    @abstractmethod
    def update_status(self): pass


