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

"""
Extracts are transformations that generate output lines from something that is not one of the input channel.
As it will yield all data for each input row, the input given is usually only one empty line.

"""
from rdc.etl.hash import Hash

from rdc.etl.io import STDIN
from rdc.etl.transform import Transform


class Extract(Transform):
    """Base class for extractions.

    .. attribute:: stream_data

        Generator, iterable or iterable-typed callable that is used as the data source. Often used as a shortcut to
        make fast prototypes of ETL processes from a dictionary, before going further with real data sources.

        Each iterator value should be something Hash.copy() can take as an argument.

    Using a simple dict::

        >>> my_stream_data = ({'foo': 'bar'}, {'foo': 'baz'}, )
        >>> t = Extract(stream_data=my_stream_data)
        >>> list(t(Hash()))
        [<Hash {'foo': 'bar'}>, <Hash {'foo': 'baz'}>]

    Using a callable::

        >>> def my_stream_data():
        ...     return (
        ...         {'bar': 'baz'},
        ...         {'bar': 'boo'},
        ...     )
        >>> my_stream_data = Extract(stream_data=my_stream_data)
        >>> print list(my_stream_data(Hash()))
        [<Hash {'bar': 'baz'}>, <Hash {'bar': 'boo'}>]

    Using a generator::

        >>> def my_stream_data():
        ...     yield {'bar': 'baz'}
        ...     yield {'bar': 'boo'}
        >>> my_stream_data = Extract(stream_data=my_stream_data)
        >>> print list(my_stream_data(Hash()))
        [<Hash {'bar': 'baz'}>, <Hash {'bar': 'boo'}>]

    .. note::

        You can apply Extract directly as a decorator::

            @Extract
            def my_stream_data():
                # ...

    """

    stream_data = []

    def __init__(self, stream_data=None):
        super(Extract, self).__init__()

        self.stream_data = stream_data or self.stream_data

        if hasattr(stream_data, '__name__'):
            self.__name__ = self.stream_data.__name__

    def transform(self, hash, channel=STDIN):
        stream_data = self.stream_data() if callable(self.stream_data) else self.stream_data

        if stream_data:
            for data in stream_data:
                yield hash.copy(data)


