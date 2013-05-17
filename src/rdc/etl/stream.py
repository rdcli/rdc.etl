# -*- coding: utf-8 -*-
#
# Author: Romain Dorgueil <romain@dorgueil.net>
# Copyright: © 2011-2013 SARL Romain Dorgueil Conseil
#
from rdc.etl.hash import Hash

class EndOfStream(Exception):
    pass

class _StreamToken(object):
    def __init__(self, name):
        self.__name = name
    def __repr__(self):
        return '<%s>' % (self.__name, )

class Stream(object):
    EOS = _StreamToken('EOS')

    def __init__(self):
        self.reset()
        self.pipes = []

    @classmethod
    def create_initializer(cls):
        s = cls()
        s.write(Hash())
        s.end()
        return s

    def reset(self):
        self.__data = []
        self.__eos_received = False

    def create_pipe(self):
        pipe = self.__class__()
        self.pipes.append(pipe)
        return pipe

    def write(self, item):
        #print 'write', self, item

        if self.__eos_received:
            if item is self.EOS:
                return # TODO stop ignoring double EOS
            raise RuntimeError('You cannot write to a stream that already received EOS.')

        if item == self.EOS:
            self.__eos_received = True
        else:
            assert isinstance(item, Hash), 'Stream was expecting a Hash, got '+repr(item)

        self.__data.append(item)

        if len(self.pipes):
            self.flush()

#    def write_many(self, *items):
#        for item in items:
#            self.write(item)

    def flush(self):
        while len(self):
            item = self.read()
            for pipe in self.pipes:
                if self.EOS == item:
                    pipe.end()
                else:
                    pipe.write(item.copy())

    def read(self):
        #print 'read', self
        data_read, self.__data = self.__data[0], self.__data[1:]
        return data_read

    def end(self):
        self.write(self.EOS)

    def __len__(self):
        return len(self.__data)

    @property
    def eos_received(self):
        return self.__eos_received

    @property
    def alive(self):
        return bool(len(self) or not self.__eos_received)

    def __repr__(self):
        return '<%s len=%d eos_received=%s alive=%s id=%s>' % (self.__class__.__name__, len(self), int(self.eos_received), int(self.alive), id(self))

