Jobs
====

Concept
:::::::

*The Scheduler and the Overseer*

Jobs, (previsouly *harness*), are the glue that ties transformations together and let them interract.

>>> job = Job()

Jobs have a few purposes:

* **Manage the graph**.  and their input/output channels and connections.

>>> # Add a transform. Each transform has its own thread. You should avoid using the lower level method ``add()``
>>> # unless you perfectly understand the underlying mechanisms.
>>> job.add_chain(t1, t2, t3)

* **Manage threads and work units**. Each transform is contained in a thread that will live from the job start to
  whatever means that the contained transform is now "dead". The job will dispatch work between those threads, and
  monitor their status.

>>> # Show thread status
>>> print '\n'.join(map(repr, h.get_threads()))
(1, - Extract-1 in=1 out=3)
(2, - SimpleTransform-2 in=3 out=3)
(3, - Log-3 in=3 out=3)

The format of the tuples shown is the following:

    (``id``, ``state`` ``name`` ``statistics``)

``Id`` is a simple numeric identifier that indexes the transform and associated thread. ``State`` is either "+" for "alive
thread" or "-" for "finished/dead thread". ``Name`` is the thread name, most often built using the transform name and a
thread id. ``Statistics`` is the number of lines that got read or written to input / output on this transform.

* **Manage execution**. Once configured, your ETL process will be runnable by calling the job instance.

>>> # Call the job == run the ETL process
>>> job()

API
:::

.. currentmodule:: rdc.etl.harness.base
.. autoclass:: IHarness

.. currentmodule:: rdc.etl.job
.. autoclass:: Job

    .. automethod:: add_chain
    .. automethod:: get_threads
    .. automethod:: get_transforms
    .. automethod:: __call__
