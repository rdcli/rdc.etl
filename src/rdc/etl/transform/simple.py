# -*- coding: utf-8 -*-
#
# Author: Romain Dorgueil <romain@dorgueil.net>
# Copyright: © 2011-2013 SARL Romain Dorgueil Conseil
#
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
    def __init__(self, getter = None, *filters):
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
        self.conditions.insert(0, lambda hash, name: hash.get(field or name, None) is None)
        return self

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
            try:
                value = _apply_filter(value, hash, filter)
            except Exception, e:
                print hash, _name, value, filter
                raise

        return value

class SimpleTransform(Transform):
    DescriptorClass = _SimpleItemTransformationDescriptor

    def __init__(self, *filters):
        super(SimpleTransform, self).__init__()
        self._filters = list(filters)

    def transform(self, hash):
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

    def remove(self, name):
        self.filter(lambda t: t.remove(name))

    def filter(self, filter):
        self._filters.append(filter)
        return self