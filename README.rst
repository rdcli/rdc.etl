=======
rdc.etl
=======

``rdc.etl`` is an Extract Transform Load (ETL) toolkit for python 2.7+ (no
python 3 support yet). It gives you all the tools needed to create complex data
integration jobs from simple atomic io-connected transformation blocks.

.. image:: https://secure.travis-ci.org/rdcli/etl.png
  :target: http://travis-ci.org/rdcli/etl
  :alt: Build Status


Documentation
-------------

`Full documentation for rdc.etl on ReadTheDocs <http://etl.rdc.li/>`_.

This is a work in progress, the 1.0 API may change until 1.0 is released.


Example usage
-------------

.. code-block:: python

    >>> # Sample data extract transformation.
    >>> # Use hardcoded data here for sample purpose.
    >>> from rdc.etl.transform.extract import Extract
    >>> @Extract
    ... def sample_extract():
    ...     yield {'first_name': 'John', 'last_name': 'Doe', }
    ...     yield {'first_name': 'Jane', 'last_name': 'Dae', }


.. code-block:: python

    >>> # Sample data transformation.
    >>> from rdc.etl.transform import Transform
    >>> @Transform
    ... def sample_transform(hash, channel):
    ...     hash['last_name'] = hash['last_name'].upper()
    ...     hash['initials'] = '{0}.{1}.'.format(hash['first_name'][0], hash['last_name'][0]).upper()
    ...     yield hash


.. code-block:: python

    >>> # Sample load. This is only a screen log for sample purpose.
    >>> from rdc.etl.transform.util import Log
    >>> sample_load_to_screen = Log()


.. code-block:: python

    >>> # Tie everything together, then run!
    >>> from rdc.etl.harness.threaded import ThreadedHarness
    >>> job = ThreadedHarness()
    >>> job.add_chain(sample_extract, sample_transform, sample_load_to_screen)
    >>> job()


Running the Test Suite
----------------------

.. code-block:: shell

    pip install nose
    make test


Release Notes
-------------

1.0.0a5
.......

* Status: console now has amazing ansi, detailed io statistics, overall stats
  (memory, time) added, experimental http status, db stats for database load.
* API stabilization, cleanup and simplification towards 1.0.0.
* Simple handling of KeyboardInterrupt: CTRL-C will now exit the running job
  instead of making your process stale.
* Maps simplification.
* Enhancements to various transform classes: load.database.DatabaseLoad,
  filter.Filter, map.xml.XmlMap, util.Log, join.database.DatabaseJoin
* New transforms: util.Limit
* Various bugfixes.
* Minor enhancements: custom names in transforms, some more tests.
* Moved repository to github.com/rdcli/etl.


Contributing
------------

I'm Romain Dorgueil.

``rdc.etl`` is on `GitHub <https://github.com/rdcli/etl>`_.

Get in touch, via GitHub or otherwise, if you've got something to contribute,
it'd be most welcome!

If you feel overwhelmingly grateful, or want to support the project you can tip
me on `Gittip <https://www.gittip.com/rdorgueil/>`_.


