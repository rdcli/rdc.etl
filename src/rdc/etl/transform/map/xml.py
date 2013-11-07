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

from __future__ import absolute_import
import string
import xml.etree.ElementTree
from rdc.etl.io import STDIN
from rdc.etl.transform import Transform
from watopy.util.html import unescape

# XXX dependency on watopy, needs removal

class XmlMap(Transform):
    field = None
    xpath = None

    def __init__(self, field=None, xpath=None):
        super(XmlMap, self).__init__()

        self.field = field or self.field
        self.xpath = xpath or self.xpath

    def get_dict_from_object(self, object):
        d = {}
        for i in object:
            if i.text is not None:
                try:
                    t = unescape(i.text.encode('raw_unicode_escape').decode('utf-8'))
                except Exception, e:
                    # XXX Handle this error
                    #raise
                    t = filter(lambda x: x in string.printable, i.text.encode('raw_unicode_escape'))
                d[i.tag] = t
            if i.tag == 'extras':
                for extra in i:
                    if extra.text:
                        d[extra.tag.lower()] = unescape(extra.text.encode('raw_unicode_escape').decode('utf-8'))
                    else:
                        d[extra.tag.lower()] = None

        if not 'description' in d:
            d['description'] = d['name']
        d['flux_id'] = d['sku']
        d['flux_group_id'] = d['sku']

        return d

    def transform(self, hash, channel=STDIN):
        xml_source = hash.get(self.field)

        if xml_source is None:
            raise KeyError(
                self.__class__.__name__ + ' transform() method was expecting to find a "' + self.field + '" ' +
                ' field, containing the XML source, but this field is not preseent in source hash (' + repr(hash) + ').'
            )

        root = xml.etree.ElementTree.fromstring(xml_source)

        if self.xpath:
            # TODO implement
            pass

        for o in root:
            yield hash.copy({self.field: o}).update(self.get_dict_from_object(o))
