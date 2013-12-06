Contributing
============

`The code is available on github <http://github.com/rdcli/etl/>`_.

.. code-block:: bash

    $ git clone https://github.com/rdcli/etl.git

The way to contribute is to fork the project in your own github account, and then make pull requests. If you don't want
to use github, you can send pull requests by mail (``git format-patch`` is your friend) to romain(at)rdc(dot)li.

It's probably a good idea to discuss ideas before starting to implement.

You're also *(more than)* very welcome to improve the documentation, or the unit tests.

The project roadmap is available below.

This package is used on live systems, and no backward incompatible feature will be implemented in 1.x after 1.0.0 has
been released (at least, we'll try). See `Semantic Versionning <http://semver.org/>`_.

Roadmap
:::::::

General
-------

* Documentation, more documentation, better documentation
* Test coverage
* Examples
* "Job" tests

Milestone 1.0
-------------

IO channels management
......................

* *(DONE)* Multiple input/output possible for each transformation, with default channels

* *(DONE)* "Converging stars" (V model), "diverging stars" (reverse V) and diamond should be possible

* See how we deal with cycles, I guess a "health check" pass is necessary to ensure that all paths have an end.

Error handling
..............

* Exceptions are sent to stdout, destroying statuses
* There should be recoverable and fatal errors
* stderr should be a special output stream that handle exceptions, and all stdouts should be plugged into some
  handler.
* errors should appear in status
* React to Control-C (KeyboardInterrupt)

Milestone 1.1
-------------

Services/Connections/...
........................

* what is a good name for this ?
* databases, webservices, filesystems, http, ...
* stats (r/w)

Display/status
..............

* Better Log() (nice tables wanted)
* wsgi status ? (html) mail status ?
* Catchall for unplugged IO channels ? For example, all messages going to unplugged STDERR channels could be sent to a
  given transform, so we can act (email ...)

Milestone 1.2
-------------

* Whatever will be needed at this time, let's focus on first versions for now (ideas welcome).

