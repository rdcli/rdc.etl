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

import pprint
from rdc.etl.transform.extract import Extract
from rdc.etl.transform.simple import SimpleTransform
from rdc.etl.harness.threaded import ThreadedHarness as Harness

def _get_value(id):
    return 'Value for %d' % (id, )

def build_producer(name, count=3, get_value=None, value_name='value'):
    return Extract([{'producer': name, 'id': count - i, value_name: (get_value or _get_value)(count - i)} for i in range(0, count)])

def build_simple_transform(f='upper'):
    t = SimpleTransform()
    t.add('producer').filter(f)
    return t

def run(harness):
    print
    retval = harness()
    print
    print 'Transformations (with post execution state):'
    print '\n'.join(['  %s' % line for line in pprint.pformat(harness._transforms).split('\n')])
    print '  -> return ', retval
    print

__all__ = [build_producer, build_simple_transform, run, Harness, ]