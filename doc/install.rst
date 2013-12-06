Install
=======

Using PyPI
::::::::::

The project is currently marked as alpha. It's available on PyPI, but you need to specify a version spec for pip to
find it:

.. code-block:: bash

    $ pip install rdc.etl==1.0.0a3

You can also ask for the latest version:

.. code-block:: bash

    $ pip install rdc.etl\>1.0.0a

You should be done. You can check in a python shell that it worked.

>>> from rdc.etl import __version__
>>> print __version__

Using git
:::::::::

You can also install ``rdc.etl`` from sources, using git. Depending on what you want to do, you can either use ``master``
branch which contains the latest stable code (aka what is published to PyPI), or the ``dev`` branch (aka the target
of incoming cool features).

.. code-block:: bash

    $ git clone https://github.com/rdcli/etl.git
    $ cd etl
    $ python setup.py develop

.. note::

    Virtualenv usage is highly advised.

