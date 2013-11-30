Maps
====

.. automodule:: rdc.etl.transform.map

Map (base class and decorator)
::::::::::::::::::::::::::::::

.. module:: rdc.etl.transform.map
.. autoclass:: Map

Example usage::

    >>> @Map
    >>> def my_map(s):
    ...    for l in s.split('\n'):
    ...        yield {'f%d' % i: v for i, v in enumerate(l.split(':'))}
    ...
    >>> my_map((Hash({'_': 'a:b:c\nb:c:d\nc:d:e'}), ))
    [<Hash {'f0': 'a', 'f1': 'b', 'f2': 'c'}>, <Hash {'f0': 'b', 'f1': 'c', 'f2': 'd'}>, <Hash {'f0': 'c', 'f1': 'd', 'f2': 'e'}>]

CsvMap
::::::

.. module:: rdc.etl.transform.map.csv
.. autoclass:: CsvMap

XmlMap
::::::

.. module:: rdc.etl.transform.map.xml
.. autoclass:: XmlMap

