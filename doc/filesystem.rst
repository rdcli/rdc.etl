Filesystem
==========

Not really implemented, would like some abstraction for this.

You can use FileExtract to read a file into a field.

.. code-block:: python

    t = FileExtract('/tmp/filename', output_field='_content')
    job.add_chain(t)

If you don't need to keep a lot of different things, you can use the default output_field (subject, context) that is
``_``. It can be handy as transforms that only act on one field will read this one by default.

.. code-block:: python

    t1 = FileExtract('/tmp/file.csv')
    t2 = CsvMap()
    job.add_chain(t1, t2)
