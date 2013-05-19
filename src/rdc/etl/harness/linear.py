# -*- coding: utf-8 -*-
#
# Author: Romain Dorgueil <romain@dorgueil.net>
# Copyright: © 2011-2013 SARL Romain Dorgueil Conseil
#
import types

from rdc.etl.harness import AbstractHarness
from rdc.etl.util import Timer

raise RuntimeError('Need update ...')

class LinearHarness(AbstractHarness):
    def process(self, stream, current):
        id_current = id(current)
        eos = []
        if not id_current in self.connections:
            return stream

        transforms = self.connections[id_current]
        for transform in transforms:
            stream_out = []
            timer = Timer()
            with timer:
                for item in stream:
                    _out = transform(item.copy())
                    if isinstance(_out, types.GeneratorType):
                        stream_out.extend(_out)
                    elif _out is not None:
                        stream_out.append(_out)
                eos.append(self.process(stream_out, transform))
            print '    >', repr(transform), '(' + str(timer) +')'

        if len(eos) == 1: eos = eos.pop()

        return eos
