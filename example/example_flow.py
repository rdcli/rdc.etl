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

from rdc.etl.extra.example import Harness, build_producer, run
from rdc.etl.io import STDIN2
from rdc.etl.status.console import ConsoleStatus
from rdc.etl.transform.flow.sort import Sort
from rdc.etl.transform.flow.sortedjoin import SortedJoin
from rdc.etl.transform.util import Log

print('#################')
print('# Software sort #')
print('#################')
print
print('Producer -> Sort -> Log')

h = Harness()
h.status.append(ConsoleStatus())
p1 = build_producer('Producer 1')
h.add_chain(p1, Sort(key=('id',)), Log())
run(h)

print('###############')
print('# Sorted Join #')
print('###############')
print
print("Producer1 -> Sort -(stdin)---> SortedJoin --> Log")
print("Producer2 -> Sort -(stdin2)-'")

h = Harness()
h.status.append(ConsoleStatus())
p1 = build_producer('Producer 1')
p2 = build_producer('Producer 2', get_value=lambda id: int(id) * 42, value_name='price')
sj = SortedJoin(key=('id', ))
h.add_chain(p1, Sort(key=('id',)), sj, Log())
h.add_chain(p2, Sort(key=('id',)), output=(sj, STDIN2, ))
run(h)

