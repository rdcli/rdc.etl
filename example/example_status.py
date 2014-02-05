from rdc.etl.extra.example import Harness, build_producer, run
from rdc.etl.extra.simple import SimpleTransform
from rdc.etl.transform.util import Log

print('########################')
print('# Simplest ETL process #')
print('########################')
print
print('Producer -> Transform -> Log')

h = Harness()
p = build_producer('Producer')
t = SimpleTransform()
h.add_chain(p, t, Log())
run(h)

print '\n'.join(map(repr, h.get_threads()))


