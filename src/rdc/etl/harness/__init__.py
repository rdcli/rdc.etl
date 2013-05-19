# -*- coding: utf-8 -*-
#
# Author: Romain Dorgueil <romain@dorgueil.net>
# Copyright: © 2011-2013 SARL Romain Dorgueil Conseil
#

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
        _value = self.loop()
        self.finalize()
        return _value

    @abstract
    def loop(self):
        pass

    @abstract
    def update_status(self):
        pass
