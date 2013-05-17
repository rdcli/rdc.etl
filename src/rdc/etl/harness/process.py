from __future__ import absolute_import

import types
from processing.process import Process
from processing.queue import Queue

from rdc.etl.transform import Transform

EndOfQueue = object()

class ProcessTransformDecorator(Transform, Process):
    def __init__(self, transform):
        Process.__init__(self)

        self.transform = transform

        self.input = Queue()
        self.input.put(EndOfQueue)
        self.output = Queue()

    def run(self):
        while True:
            _in = self.input.get()

            if _in == EndOfQueue:
                self.output.put(EndOfQueue)
                break

            _out = self(_in)

            if isinstance(_out, types.GeneratorType):
                for item in _out:
                    print 'out item', item
                    self.output.put(item)
            elif _out is not None:
                self.output.put(_out)

    def __call__(self, hash):
        return self.transform.__call__(hash)

    def transform(self, hash):
        return self.transform.transform(hash)


