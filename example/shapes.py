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

def run(harness):
    print
    pprint.pprint(h._transforms)
    print
    retval = h()
    print '  -> return ', retval
    print

print('################')
print('# Linear shape #')
print('################')
print
print('Producer -> SimpleTransform -> Log')
h = Harness()
p1 = build_producer('producer 1')
h.add_chain(p1, build_simple_transform(), Log())
run(h)

print('###############')
print('# Split shape #')
print('###############')
print
print('Producer -> Split -> SimpleTransform1 -> Log1')
print('                 `-> SimpleTransform2 -> Log2')
h = Harness()

