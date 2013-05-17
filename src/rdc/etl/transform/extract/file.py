# -*- coding: utf-8 -*-
#
# Author: Romain Dorgueil <romain@dorgueil.net>
# Copyright: © 2011-2013 SARL Romain Dorgueil Conseil
#
from werkzeug.utils import cached_property
from rdc.etl.transform import Transform
from rdc.etl.util import create_http_reader, create_file_reader


class FileExtract(Transform):
    uri = None
    output_field = None

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

    def transform(self, hash):
        hash.set(self.output_field, self.content)
        yield hash

