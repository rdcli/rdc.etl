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

from abc import ABCMeta, abstractmethod
from rdc.etl.error import AbstractError


class IStatus:
    __metaclass__ = ABCMeta

    @abstractmethod
    def initialize(self, harness, debug, profile):
        """Initialize status."""
        raise AbstractError(self.initialize)

    @abstractmethod
    def update(self, harness, debug, profile):
        """update this status"""
        raise AbstractError(self.update)

    @abstractmethod
    def finalize(self, harness, debug, profile):
        """Finalize status."""
        raise AbstractError(self.finalize)


class BaseStatus(IStatus):
    def initialize(self, harness, debug, profile):
        pass

    def update(self, harness, debug, profile):
        raise AbstractError(self.update)

    def finalize(self, harness, debug, profile):
        pass


