Kickstart
=========

A fast 4 step overview to get you kickstarted into rdc.etl.

Create a Harness
::::::::::::::::

* Connect transformations
* Runnable
* Manage threading

>>> from rdc.etl.harness.threaded import ThreadedHarness as Harness
>>> harness = Harness()
>>> # ... add your transforms here (see below)
>>> harness() # run!


Create transformations
::::::::::::::::::::::

* Various types, some transformations takes input and yield output, while some
  are input-only ("load") or output-only ("extract").

>>> from rdc.etl.transform.extract import Extract
>>> extract = Extract(stream_data=({'foo': 'bar'}, {'foo': 'baz'}))

`Extract` is a base class to write extract transformations, and we'll produce
rows from a tuple of dictionaries here. Real life would usually use databases,
webservices, files ...

>>> from rdc.etl.transform.simple import SimpleTransform
>>> transform = SimpleTransform()
>>> transform.add('foo').filter('upper')

`SimpleTransform` is a transformation builder helper, that has a lot of
shortcut to build data transformations based on callbacks, filters and simple
tests.

>>> from rdc.etl.transform.util import Log
>>> load = Log()

`Log` is not a "load" transformation stricto sensu (as it acts as an identity
transformation, sending to the default output channel whatever comes in its
default input channel), but we'll use it as such for demonstration purpose.


Tie transformations together
::::::::::::::::::::::::::::

The default harness has a `add_chain` shortcut that allows to easily create and
connect together a linear chain of transformations (by linear, we mean that
every transform default output channel is connected to the next transformation
default input channel). This is a very common case, and should be enough to
play around a bit.

>>> harness.add_chain(extract, transform, load)

This code, without using the shortcut, would need to be written as follow.

>>> harness.add(extract)
>>> harness.add(transform)
>>> harness.add(load)
>>> transform._input.plug(extract._output)
>>> load._input.plug(transform._output)

This API is a work in progress.


Run the job
:::::::::::

Once your job has been configured in your harness instance, you can run it.

>>> harness()

