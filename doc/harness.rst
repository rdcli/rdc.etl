Harnesses
=========

*The scheduler and the overseer*

>>> harness = Harness()

Harnesses have a few purposes:

* **Manage the transformations graph**, and their input/output channels and connections.

>>> # Add a transform. Each transform has its own thread
>>> harness.add(t)

* **Manage threads**. Each transform is contained in a thread that will live from the harness call to whatever means that
  the contained transform is now "dead".

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

* **Manage ETL process execution**. Once configured, your ETL process will be runnable by calling the harness.

>>> # Call the harness == run the ETL process
>>> harness()


