from rdc.etl.status.console import ConsoleStatus
from rdc.etl.transform.util import Log
from rdc.etl.transform.extract import Extract
from rdc.etl.harness.threaded2 import ThreadedHarness as ThreadedHarness2
from rdc.etl.harness.threaded import ThreadedHarness

def build_producer(name):
    return Extract(({'producer': name, 'id': 1}, {'producer': name, 'id': 2}))

for Harness in ThreadedHarness, ThreadedHarness2:
    print
    print "-------------------------------"
    print "With %r" % Harness
    print
    print
    h = Harness()
    h.status.append(ConsoleStatus())
    p1 = build_producer('p1')
    h.chain_add(p1, Log())
    h()
    print
    print

