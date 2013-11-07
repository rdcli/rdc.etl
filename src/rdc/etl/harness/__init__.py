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

class IHarness(object):
    """
    ETL harness interface.

    The harness is basically the executable stuff that will actually run a job.

    """

    @abstract
    def __call__(self):
        pass

    def initialize(self):
        pass

    def finalize(self):
        pass

class AbstractHarness(IHarness):
    """
    Abstract harness defines initialize/finalize/loop, which are pretty handy. If you implement a custom harness, there
    is 99.9% chances you want to extend this or a subclass of this.

    """
    def __init__(self):
        self.status = []

    def __call__(self):
        self.initialize()
        self.validate()
        _value = self.loop()
        self.finalize()
        return _value

    @abstract
    def loop(self):
        pass

    @abstract
    def update_status(self):
        pass

    def validate(self):
        pass
