# -*- coding: utf-8 -*-
#
# Author: Romain Dorgueil <romain@dorgueil.net>
# Copyright: © 2011-2013 SARL Romain Dorgueil Conseil
#
from __future__ import absolute_import
import string
import xml.etree.ElementTree
from rdc.etl.transform import Transform
from watopy.util.html import unescape


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

    def transform(self, hash):
        root = xml.etree.ElementTree.fromstring(hash.get(self.field))

        if self.xpath:
            # TODO implement
            pass

        for o in root:
            yield hash.copy({self.field: o}).update(self.get_dict_from_object(o))
