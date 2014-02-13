from rdc.etl import H
from rdc.etl.extra.util import TransformBuilder
from rdc.etl.job import Job
from rdc.etl.transform.extract import Extract
from rdc.etl.transform.util import Log

@TransformBuilder(Extract)
def ExampleExtract():
    yield H(('name', 'foo', ),
            ('value', 'lorem', ),)
    yield H(('name', 'bar', ),
            ('value', 'ipsum', ),)
    yield H(('name', 'baz', ),
            ('value', 'dolor', ),)

class ExampleJob(Job):
    def initialize(self):
        self.add_chain(
            ExampleExtract(),
            Log(),
        )
