Transformations
===============

.. automodule:: rdc.etl.transform

Transformations are the basic bricks to build ETL processes. Basically, it gets lines from its ``input`` and sends
transformed lines to its ``output``.

You're highly encouraged to use the ``rdc.etl.transform.Transform`` class as a base for your custom transforms, as it
defines the whole :doc:`I/O logic <io>`. All transformations provided by the package are subclasses of
``rdc.etl.transform.Transform``.

.. autoclass:: Transform

**Builtin transformations reference**

.. toctree::
    :maxdepth: 2

    reference/extract
    reference/load
    reference/map
    reference/filter
    reference/join
    reference/util
    reference/flow


**Design notes**

.. toctree::
    :maxdepth: 2

    io

