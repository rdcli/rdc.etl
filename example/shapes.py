<<<<<<< HEAD
import pprint
from rdc.etl.status.console import ConsoleStatus
from rdc.etl.transform.simple import SimpleTransform
from rdc.etl.transform.util import Log
from rdc.etl.transform.extract import Extract
from rdc.etl.harness.threaded2 import ThreadedHarness as ThreadedHarness2
from rdc.etl.harness.threaded import ThreadedHarness

def build_producer(name):
    return Extract(({'producer': name, 'id': 1}, {'producer': name, 'id': 2}))

print '>>> Test of simple linear shape'

for Harness in ThreadedHarness, ThreadedHarness2:
    print "With %r" % Harness
    h = Harness()
    p1 = build_producer('p1')
    t = SimpleTransform()
    t.add('producer').filter('upper')
    h.chain_add(p1, t, Log())
    h()

    print 'Summary:'
    pprint.pprint(h._transforms)
    print '\n'
