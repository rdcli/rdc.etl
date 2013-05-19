# -*- coding: utf-8 -*-
#
# Author: Romain Dorgueil <romain@dorgueil.net>
# Copyright: © 2011-2013 SARL Romain Dorgueil Conseil
#

from rdc.etl.transform import Transform


class Extract(Transform):
    """
    Base class for data producers.

    It contains a simple implementation that will make an iterable into the output stream.

    >>> from rdc.etl.hash import Hash
    >>> t = Extract(stream_data=({'foo': 'bar'}, {'foo': 'baz'}, ))
    >>> print list(t(Hash()))
    [<Hash {'foo': 'bar'}>, <Hash {'foo': 'baz'}>]

    It supports both iterable stream data and callable stream data (in the last case, the callable must return an
    iterable).

    >>> def get_iterator():
    ...     yield {'bar': 'baz'}
    ...     yield {'bar': 'boo'}
    >>> t = Extract(stream_data=get_iterator)
    >>> print list(t(Hash()))
    [<Hash {'bar': 'baz'}>, <Hash {'bar': 'boo'}>]

    Please note that as the extractor will want to transform a Hash into another Hash, the stream_data iterator elements
    should be some kind of dict (and at least, they should be able to be passed to Hash.copy)

    """

    stream_data = []

    def __init__(self, stream_data=None):
        super(Extract, self).__init__()

        self.stream_data = stream_data or self.stream_data

    def transform(self, h):
        stream_data = self.stream_data() if callable(self.stream_data) else self.stream_data

        for data in stream_data:
            out = h.copy(data)
            yield out
