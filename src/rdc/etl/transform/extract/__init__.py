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

from rdc.etl.io import STDIN
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

    def transform(self, hash, channel=STDIN):
        stream_data = self.stream_data() if callable(self.stream_data) else self.stream_data

        if stream_data:
            for data in stream_data:
                yield hash.copy(data)
