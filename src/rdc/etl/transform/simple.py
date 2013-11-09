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
from rdc.etl.util import filter_html


def _apply_filter(value, hash, filter):
    # method filter
    if isinstance(filter, str):
        return getattr(value, filter)()

    # multi filter (gets the hash along with value
    elif hasattr(filter, '_is_multi') and filter._is_multi:
        return filter(value, hash)

    # simple standard filter, just a callable that transforms a value
    return filter(value)


class _SimpleItemTransformationDescriptor(object):
    def __init__(self, getter=None, *filters):
        self.getter = getter
        self.filters = list(filters)
        self.conditions = []

    def filter(self, filter):
        self.filters.append(filter)
        return self

    def filter_multi(self, filter):
        filter._is_multi = True
        self.filters.append(filter)
        return self

    def save_as(self, k, filter=None):
        """ Save the currently filtered value in another field, in its current state. You can still apply filters after
        this, but it's used to persist a partially transformed value.

        :param k: key to save under
        :return:
        """

        def _filter(v, h, k=k, filter=filter):
            h.set(k, _apply_filter(v, h, filter) if filter is not None else v)
            return v

        return self.filter_multi(_filter)

    def filter_html(self):
        self.filters.append(filter_html)
        return self

    def if_none(self, field=None):
        """
        TODO document this and add a generic if_
        """
        self.conditions.insert(0, lambda hash, name: hash.get(field or name, None) is None)
        return self

    def prepend(self, *fields, **options):
        cond = options.get('cond', None)
        postfix = options.get('postfix', None)
        separator = options.get('separator', '')

        # default conditions
        if cond is None:
            cond = lambda v: v and len(v)
        elif not callable(cond):
            cond = lambda v: cond

        def _filter(v, h, fields=fields, cond=cond, postfix=postfix, separator=separator):
            out = separator.join([h.get(field) for field in fields if cond(h.get(field))])
            if len(out) and postfix:
                out = out + postfix
            return out + (v or '')

        return self.filter_multi(_filter)

    def append(self, *fields, **options):
        cond = options.get('cond', None)
        prefix = options.get('prefix', None)
        separator = options.get('separator', '')

        # default conditions
        if cond is None:
            cond = lambda v: v and len(v)
        elif not callable(cond):
            cond = lambda v: cond

        def _filter(v, h, fields=fields, cond=cond, prefix=prefix, separator=separator):
            out = separator.join([h.get(field) for field in fields if cond(h.get(field))])
            if len(out) and prefix:
                out = prefix + out
            return (v or '') + out

        return self.filter_multi(_filter)

    def set_getter(self, getter):
        self.getter = getter
        return self

    def __call__(self, hash):
        if isinstance(self.getter, str):
            _name = self.getter
            getter = lambda o: o.get(_name)
            getter.func_name = 'get_' + str(_name)
        elif isinstance(self.getter, unicode):
            _name = self.getter.encode('utf-8')
            getter = lambda o: o.get(_name)
            getter.func_name = 'get_' + str(_name)
        else:
            getter = self.getter
            _name = repr(self.getter)

        value = getter(hash)

        for filter in self.filters:
            value = _apply_filter(value, hash, filter)

        return value


class SimpleTransform(Transform):
    """SimpleTransform is an attempt to make a trivial transformation easy to build, using fluid APIs and a lot of easy
    shortcuts to apply filters to some fields.

    The API is not stable and this will probably go to a contrib or extra package later.

    Example:

        >>> t = SimpleTransform()

        # Apply "upper" method on "name" field, and store it back in "name" field.
        >>> t.add('name').filter('upper')

        # Apply the lambda to "description" field content, and store it into the "full_description" field.
        >>> t.add('full_description', 'description').filter(lambda v: 'Description: ' + v)

        # Remove the previously defined "useless" descriptor. This does not remove the "useless" fields into transformed
        # hashes, it is only usefull to override some parent stuff.
        >>> t.delete('useless')

        # Mark the "notanymore" field for deletion upon transform. Output hashes will not anymore contain this field./
        >>> t.remove('notanymore')

        # Add a field (output hashes will contain this field, all with the same "foo bar" value).
        >>> t.test_field = 'foo bar'

    """
    DescriptorClass = _SimpleItemTransformationDescriptor


    def __init__(self, *filters):
        super(SimpleTransform, self).__init__()
        self._filters = list(filters)

    def transform(self, hash, channel=STDIN):
        for name, value_getter in self.__dict__.items():
            if name[0] == '_':
                continue

            try:
                conditions = list(value_getter.conditions)
            except Exception, e:
                conditions = []

            can_update = True
            for condition in conditions:
                if not condition(hash, name):
                    can_update = False
                    break

            if can_update:
                if callable(value_getter):
                    hash.set(name, value_getter(hash))
                else:
                    hash.set(name, value_getter)

        for filter in self._filters:
            hash = filter(hash)

        return hash

    def add(self, name, getter=None):
        if getter is None:
            getter = name
        descr = self.DescriptorClass(getter)
        setattr(self, name, descr)
        return descr

    def delete(self, name):
        """
        Unset a descriptor.
        """
        delattr(self, name)

    def remove(self, *names):
        """
        Removes a field in hash, using a post transform filter.
        """
        for name in names:
            self.filter(lambda t: t.remove(name))

    def filter(self, filter):
        """
        Adds a post transform filter.
        """
        self._filters.append(filter)
        return self

