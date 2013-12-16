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

from rdc.etl.contrib.example import Harness, build_producer, build_simple_transform, run
from rdc.etl.io import STDOUT, STDOUT2
from rdc.etl.transform.flow.split import Split
from rdc.etl.transform.util import Log


print('################')
print('# Linear shape #')
print('################')
print
print('Producer -> SimpleTransform -> Log')

h = Harness()
p1 = build_producer('Producer 1')
h.add_chain(p1, build_simple_transform(), Log())
run(h)


print('#####################################')
print('# Split shape (2 different outputs) #')
print('#####################################')
print
print('Producer -> Split ---(stdout)--> SimpleTransform1 -> Log1')
print('                   `-(stdout2)-> SimpleTransform2 -> Log2')

h = Harness()
producer = build_producer('Producer 1', 10)
split = Split(output_selector = lambda h: h.get('id') % 2 and STDOUT2 or STDOUT)
h.add_chain(producer, split, build_simple_transform(), Log())
h.add_chain(build_simple_transform('lower'), Log(), input=(split, STDOUT2, ))
run(h)


print('###########################################')
print('# Split shape (single "multitail" output) #')
print('############################################')
print
print('Producer -(stdout)--> SimpleTransform1 -> Log1')
print('                  `-> SimpleTransform2 -> Log2')
print
print('Note: all producer output will be sent to both branch.')

h = Harness()
producer = build_producer('Producer 1', 2)
h.add_chain(producer, build_simple_transform(), Log())
h.add_chain(build_simple_transform('lower'), Log(), input=(producer, STDOUT, ))
run(h)

