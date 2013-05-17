# -*- coding: utf-8 -*-
#
# Author: Romain Dorgueil <romain@dorgueil.net>
# Copyright: © 2011-2013 SARL Romain Dorgueil Conseil
#

from rdc.etl.hash import Hash

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

    def pre_process(self): pass
    def post_process(self): pass

    def __call__(self, stream=None):
        self.pre_process()
        _value = self.process(stream or [Hash()], self.BEGIN)
        self.post_process()
        return _value

    @abstract
    def process(self, stream, current): pass

