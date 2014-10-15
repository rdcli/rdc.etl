Statuses
========

.. module:: rdc.etl.status

Statuses are the tools to observe a process execution state. Not documented yet, but try the following before you run
the job:

>>> from rdc.etl.status.console import ConsoleStatus
>>> job.status.append(ConsoleStatus())

ConsoleStatus
:::::::::::::

.. module:: rdc.etl.status.console
.. autoclass:: ConsoleStatus

