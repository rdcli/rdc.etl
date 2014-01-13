# -*- coding: utf-8 -*-
#
# Copyright 2012-2014 Romain Dorgueil
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


from rdc.etl.error import AbstractError
from rdc.etl.io import STDIN
from rdc.etl.transform import Transform


class Extract(Transform):
    """Base class for extract transforms.

    .. attribute:: extract

        Generator, iterable or iterable-typed callable that is used as the data source. Often used as a shortcut to
        make fast prototypes of ETL processes from a dictionary, before going further with real data sources.

        Each iterator value should be something Hash.copy() can take as an argument.


    Example using a dict::

        >>> from rdc.etl.transform.extract import Extract

        >>> data = ({'foo': 'bar'}, {'foo': 'baz'}, )
        >>> my_extract = Extract(extract=data)

        >>> list(my_extract({}))
        [H{'foo': 'bar'}, H{'foo': 'baz'}]


    Example using a callable::

        >>> from rdc.etl.transform.extract import Extract

        >>> @Extract
        ... def my_extract():
        ...     return (
        ...         {'bar': 'baz'},
        ...         {'bar': 'boo'},
        ...     )

        >>> list(my_extract({}))
        [H{'bar': 'baz'}, H{'bar': 'boo'}]


    Example using a generator::

        >>> from rdc.etl.transform.extract import Extract

        >>> @Extract
        ... def my_extract():
        ...     yield {'bar': 'baz'}
        ...     yield {'bar': 'boo'}

        >>> print list(my_extract({}))
        [H{'bar': 'baz'}, H{'bar': 'boo'}]

    .. note::

        Whenever you can, prefer the generator approach so you're not blocking anything while computing remaining
        elements.

    """

    extract = []

    def __init__(self, extract=None):
        super(Extract, self).__init__()

        self.extract = extract or self.extract

        if hasattr(extract, '__name__'):
            self.__name__ = self.extract.__name__

    def extract(self):
        raise AbstractError(self.extract)

    def transform(self, hash, channel=STDIN):
        extracted_data = self.extract() if callable(self.extract) else self.extract

        if extracted_data:
            for line in extracted_data:
                yield hash.copy(line)


