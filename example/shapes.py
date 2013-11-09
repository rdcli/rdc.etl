import pprint
from rdc.etl.status.console import ConsoleStatus
from rdc.etl.transform.simple import SimpleTransform
from rdc.etl.transform.util import Log
from rdc.etl.transform.extract import Extract
from rdc.etl.harness.threaded import ThreadedHarness as Harness

def build_producer(name):
    return Extract(({'producer': name, 'id': 1}, {'producer': name, 'id': 2}))

def build_simple_transform():
    t = SimpleTransform()
    t.add('producer').filter('upper')
    return t

def run(title, harness):
    print '>>> Test of simple linear shape'
    retval = h()
    print 'Summary:'
    pprint.pprint(h._transforms)
    print 'h() return value:', retval
    print '\n'

# Linear shape:
# Extract -> SimpleTransform -> Log
h = Harness()
p1 = build_producer('producer 1')
h.chain_add(p1, build_simple_transform(), Log())
run('Test of simple linear shape', h)

