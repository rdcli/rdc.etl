rdc.etl
=======

Contents:

.. toctree::
   :maxdepth: 2


Overview
=======

Extract Transform Load (ETL) toolkit for python.

Toolkit for doing data integration related work. Unlike java based tools like
talend or pentaho data-integration, this is a DIY framework, and if you're
looking for a WYSIWIG ETL engine, you should probably go back to the previously
cited ones.


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

The default harness has a `chain_add` shortcut that allows to easily create and
connect together a linear chain of transformations (by linear, we mean that
every transform default output channel is connected to the next transformation
default input channel). This is a very common case, and should be enough to
play around a bit.

>>> harness.chain_add(extract, transform, load)

This code, without using the shortcut, would need to be written as follow.

>>> _extract = harness.add(extract)
>>> _transform = harness.add(transform)
>>> _transform.set_input_from(_extract)
>>> _load = harness.add(load)
>>> _load.set_input_from(_transform)

The difference between original transformation instances and their underscore
prefixed counterpart (for example, difference between extract and _extract) is
that the underscor one is an instance `ThreadedTransform`, which is a decorator
adding threading ability _and_ io queues. You won't be able to
.`set_input_from`() on or from a non decorated transformation.

This API is a work in progress, and IO channels are being reworked. Expect
changes on this part.


Run the job
:::::::::::

Once your job has been configured in your harness instance, you can run it.

>>> harness()


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

