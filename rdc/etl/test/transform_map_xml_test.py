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
from collections import OrderedDict

import unittest
from rdc.etl.extra.unittest import BaseTestCase
from rdc.etl.hash import Hash
from rdc.etl.transform.map import Map
from rdc.etl.transform.map.xml import XmlMap


class TransformMapXmlTestCase(BaseTestCase):
    def test_base_class_decorator(self):
        @XmlMap
        def xml_map(item):
            f = lambda path: item.findtext(path)
            return OrderedDict((
                ('id', item.attrib['id'], ),
                ('name', f('name'), ),
                ('value', ';'.join((i.text for i in item.findall('values/data'))), ),
            ))
        xml_map.xpath = './path/to/items/item'

        self.assertStreamEqual(
            xml_map((Hash({'_': '''<root>
                <path>
                    <to>
                        <items>
                            <item id="one">
                                <name>foo</name>
                                <values>
                                    <data>bar</data>
                                </values>
                            </item>
                            <item id="two">
                                <name>bar</name>
                                <values>
                                    <data>baz</data>
                                </values>
                            </item>
                            <item id="three">
                                <name>baz</name>
                                <values>
                                    <data>toto</data>
                                    <data>titi</data>
                                </values>
                            </item>
                        </items>
                    </to>
                </path>
            </root>'''}))), (
                OrderedDict((('id', 'one'), ('name', 'foo'), ('value', 'bar'), ), ),
                OrderedDict((('id', 'two'), ('name', 'bar'), ('value', 'baz'), ), ),
                OrderedDict((('id', 'three'), ('name', 'baz'), ('value', 'toto;titi'), ), ),
            ))


if __name__ == '__main__':
    unittest.main()
