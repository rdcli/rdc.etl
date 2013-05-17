# -*- coding: utf-8 -*-
#
# Author: Romain Dorgueil <romain@dorgueil.net>
# Copyright: © 2011-2013 SARL Romain Dorgueil Conseil
#

class Harness(object):
    BEGIN = object()

    def __init__(self):
        self.connections = {}
        self._last_step = self.BEGIN

    def add(self, *steps):
        for step in steps:
            self.connect(self._last_step, step)
            self._last_step = step

    def connect(self, tr_from, tr_to):
        id_tr_from = id(tr_from)

        if not id_tr_from in self.connections:
            self.connections[id_tr_from] = set()

        self.connections[id_tr_from].add(tr_to)

    def initialize(self): pass
    def finalize(self): pass

    def __call__(self):
        self.initialize()
        _value = self.run()
        self.finalize()
        return _value

