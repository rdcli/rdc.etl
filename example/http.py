# -*- coding: utf-8 -*-
import time

from rdc.etl.harness.threaded import ThreadedHarness
from rdc.etl.extra.example import build_producer, run
from rdc.etl.status.console import ConsoleStatus
from rdc.etl.status.http import HttpStatus
from rdc.etl.transform import Transform

h = ThreadedHarness()
p1 = build_producer('Producer 1', count=500)

@Transform
def delay(h, c):
    time.sleep(0.2)
    yield h

@Transform
def delay2(h, c):
    time.sleep(0.5)
    yield h

h.add_chain(p1, delay, delay2)
h.status.append(ConsoleStatus())
h.status.append(HttpStatus())
run(h)
