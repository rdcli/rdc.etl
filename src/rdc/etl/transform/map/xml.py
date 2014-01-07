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

from __future__ import absolute_import
from copy import copy
from inspect import isgenerator
from rdc.etl.error import AbstractError
from rdc.etl.hash import Hash
from rdc.etl.transform.map import Map
from rdc.etl.util import etree


class XmlMap(Map):
    """
    Reads a XML and yield values for each root children.

    .. todo:: think how we want to make this flexible, xpath, etc ...
    .. warning:: This does not work, don't use (or fix before :p).

    Definitions:

        XML Item: In the context of an XmlMap, we define an XML Item as being either a children of the XML root if no
        xpath has been provided, or one item returned by the XPath provided.

    .. attribute:: map_item

        Will be called for each input XML Item, and should return a dictionary of values for this item.

    .. attribute:: field

        The input field (defined in parent).

    .. attribute:: xpath

        XPath used to select items before running them through item_map().

    """

    xpath = None

    def __init__(self, map_item=None, xpath=None, field=None):
        super(XmlMap, self).__init__()

        self.map_item = map_item or self.map_item
        self.xpath = xpath or self.xpath
        self.field = field or self.field

    def map(self, value):
        """Generator that yields mapped items.

        """
        items = etree.fromstring(value)

        if self.xpath:
            items = items.findall(self.xpath)

        for item in items:
            mapped = Hash(((self.field, item, ), ))
            value_for_item = self.map_item(item)
            if isgenerator(value_for_item):
                for _value in value_for_item:
                    yield copy(mapped).update(_value)
            else:
                try:
                    mapped.update(value_for_item)
                    yield mapped
                except TypeError, e:
                    raise TypeError('{name}.map_item(...) must be iterable.'.format(
                        name=type(self).__name__
                    ))

    def map_item(self, item):
        """Convert one matched XML item to a dictionary.

        """
        raise AbstractError(self.map_item)


