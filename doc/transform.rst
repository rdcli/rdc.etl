Transformations
===============

.. module:: rdc.etl.transform
    :synopsis: base and bundled transformations

Transformations are the basic bricks to build ETL processes. Basically, it gets lines from its ``input`` and sends
transformed lines to its ``output``.

You're highly encouraged to use he ``rdc.etl.transform.Transform`` class as a base for your custom transforms, as it
defines the whole :doc:`I/O logic <transform_io>`. All transformations provided by the package are subclasses of
``rdc.etl.transform.Transform``.

Extractions
:::::::::::

.. module:: rdc.etl.transform.extract

Extractions are a special kind of transformations that take extract data from somewhere that is not the input. As it will
extract all data for each input row, the input given is usually only one empty line.

DatabaseExtract
---------------

.. module:: rdc.etl.transform.extract.database
.. autoclass:: DatabaseExtract

FileExtract
-----------

.. module:: rdc.etl.transform.extract.file
.. autoclass:: FileExtract

Maps
::::

.. module:: rdc.etl.transform.map

Maps are transforms that will yield rows depending on the value of one input field. In association with ``FileExtract``
for example, it can parse the file content format and yield rows that have an added knowledge.

By default, maps use the topic (`_`) field for input

CsvMap
------

.. module:: rdc.etl.transform.map.csv
.. autoclass:: CsvMap

XmlMap
------

.. module:: rdc.etl.transform.map.xml
.. autoclass:: XmlMap

SimpleTransform
:::::::::::::::

.. module:: rdc.etl.transform.simple
.. autoclass:: SimpleTransform

Decorator
:::::::::

.. module:: rdc.etl.transform.decorator
.. function:: transformation

Filter
::::::

Filters remove some lines from the flux.

.. module:: rdc.etl.transform.filter
.. autoclass:: SimpleFilter

Utilities
:::::::::

.. module:: rdc.etl.transform.util

Helper and utility transformations.

Log
---

.. autoclass:: Log

Stop
----

.. autoclass:: Stop

Override
--------

.. autoclass:: Override

Clean
-----

.. autoclass:: Clean

