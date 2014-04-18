Kickstart
===========

To get started, you should also read pragmatic examples in the :doc:`cookbook/index`.


Create an empty project
=======================

If you want to bootstrap an ETL project on your computer, you can now do it using the provided PasteScript template.

.. code-block:: shell

    pip install PasteScript
    paster create -t etl_project MyProject


Overview
========

Create transformations
::::::::::::::::::::::

Extract some data ...
---------------------

``Extract`` is a flexible base class to write extract transformations. We use a generator here, real life
would usually use databases, webservices, files ...

.. code-block:: python

    from rdc.etl.transform.extract import Extract

    @Extract
    def my_extract():
        yield {'foo': 'bar', 'bar': 'min'}
        yield {'foo': 'boo', 'bar': 'put'}


:doc:`For more informations, see the extracts reference <transform/reference/extract>`.

Distort it ...
--------------

``Transform`` is a flexible base class for all kind of transformations.

.. code-block:: python

    from rdc.etl.transform import Transform

    @Transform
    def my_transform(hash, channel):
        yield hash.update({
            'foo': hash['foo'].upper()
        })


:doc:`For more informations, see the transformations reference <transform/index>`.

Load it ...
-----------

We'll use the screen as our load target ...

.. code-block:: python

    from rdc.etl.transform.util import Log

    my_load = Log()


:doc:`For more informations, see the loads reference <transform/reference/load>`.

.. note::

    `Log` is not a "load" transformation stricto sensu (as it acts as an identity
    transformation, sending to the default output channel whatever comes in its
    default input channel), but we'll use it as such for demonstration purpose.


Build your job
::::::::::::::

* Connect transformations
* Runnable
* Manage threading

.. code-block:: python

    from rdc.etl.job import Job

    job = Job()


Tie transformations together
::::::::::::::::::::::::::::

The ``Job`` has a ``add_chain()`` method that can be used to easily plug a list of ordered transformations together.

.. code-block:: python

    job.add_chain(my_extract, my_transform, my_load)


Run the job
:::::::::::

Our job is ready, you can run it.

.. code-block:: python

    job()


