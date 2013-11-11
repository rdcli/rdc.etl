import pprint
from rdc.etl.io import STDOUT, STDOUT2
from rdc.etl.status.console import ConsoleStatus
from rdc.etl.transform.simple import SimpleTransform
from rdc.etl.transform.split import Split
from rdc.etl.transform.util import Log
from rdc.etl.transform.extract import Extract
from rdc.etl.harness.threaded import ThreadedHarness as Harness

def build_producer(name, count=3):
    return Extract([{'producer': name, 'id': i + 1} for i in range(0, count)])

def build_simple_transform(f='upper'):
    t = SimpleTransform()
    t.add('producer').filter(f)
    return t

def run(harness):
    print
    retval = h()
    print
    pprint.pprint(h._transforms)
    print '  -> return ', retval
    print


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
print('Producer -(stdout)-> SimpleTransform1 -> Log1')
print('        `-(stdout)-> SimpleTransform2 -> Log2')
print
print('Note: all producer output will be sent to both branch.')

h = Harness()
producer = build_producer('Producer 1', 3)
h.add_chain(producer, build_simple_transform(), Log())
h.add_chain(build_simple_transform('lower'), Log(), input=(producer, STDOUT, ))
run(h)

