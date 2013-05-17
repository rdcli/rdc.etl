# -*- coding: utf-8 -*-
#
# Author: Romain Dorgueil <romain@dorgueil.net>
# Copyright: © 2011-2013 SARL Romain Dorgueil Conseil
#
from rdc.etl.transform import Transform


class SimpleFilter(Transform):
    filter = None
    def __init__(self, filter=None):
        super(SimpleFilter, self).__init__()
        self.filter = filter or self.filter

    def transform(self, hash):
        if not self.filter or not callable(self.filter):
            raise RuntimeError('No callable provided to ' + self.__class__.__name__ + '.')

        if self.filter(hash):
            yield hash

