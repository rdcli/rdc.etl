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

from werkzeug.utils import cached_property
from rdc.etl.io import STDIN
from rdc.etl.transform import Transform
from rdc.etl.util import create_http_reader, create_file_reader


class FileExtract(Transform):
    """Deprecated in favor of FileJoin (XXX note to myself: why ?)"""

    uri = None
    output_field = '_'

    def __init__(self, uri=None, output_field=None):
        super(FileExtract, self).__init__()

        self.uri = uri or self.uri
        self.output_field = output_field or self.output_field

    @cached_property
    def reader(self):
        if self.uri is None:
            raise RuntimeError('No URI configured in ' + self.__class__.__name__ + ' transformation.')

        if self.uri.find('http://') == 0 or self.uri.find('https://') == 0:
            return create_http_reader(self.uri)
        else:
            return create_file_reader(self.uri)

    @cached_property
    def content(self):
        return self.reader()

    def transform(self, hash, channel=STDIN):
        yield hash.set(self.output_field, self.content)

