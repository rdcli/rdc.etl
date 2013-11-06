import os, sys
from setuptools import setup, find_packages

# Force our version to be used, don't know what is best practice here.
sys.path.insert(0, os.path.join(os.getcwd(), 'src'))
from rdc.etl import __version__

setup(name='rdc.etl',
      version=__version__,
      description="Extract Transform Load (ETL) toolkit for python",
      long_description="""
      Toolkit for doing data integration related work, using connected
      transformations. Unlike java based tools like talend or pentaho
      data-integration, this is a DIY framework, and if you're looking for a
      WYSIWIG ETL engine, you should probably go back to the previously cited
      ones.

      Not so relevant example:

          >>> from rdc.etl.harness.threaded import ThreadedHarness as Harness
          >>> harness = Harness()
          >>> from rdc.etl.transform.extract import Extract
          >>> extract = Extract(stream_data=({'foo': 'bar'}, {'foo': 'baz'}))
          >>> from rdc.etl.transform.simple import SimpleTransform
          >>> transform = SimpleTransform()
          >>> transform.add('foo').filter('upper')
          >>> from rdc.etl.transform.util import Log
          >>> load = Log()
          >>> harness.chain_add(extract, transform, load)
          >>> harness()

      This is a work in progress, although it it used for a few different
      production systems, it may or may not fit your need, and you should
      expect to have to dive into the code for now, as neither documentation or
      tests are there to help.

      """,
      classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'Intended Audience :: Developers',
        'License :: Other/Proprietary License',
        'Operating System :: OS Independent',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Topic :: Database',
        'Topic :: Utilities',
        'Topic :: Text Processing :: Filters',
        'Topic :: Text Processing :: Indexing',

      ],
      keywords='',
      author='Romain Dorgueil',
      author_email='romain@dorgueil.net',
      url='https://rdc.li/etl/',
      download_url='https://github.com/rdconseil/etl/tarball/' + __version__,
      license='Apache License, Version 2.0',
      packages=find_packages('src', exclude=['ez_setup', 'example', 'test']),
      package_dir = {'': 'src'},
      include_package_data=True,
      zip_safe=False,
      install_requires=[
        # todo most of the deps are optional and depends on what we wanna
        # actually use, make it so nothing is required, but still travis
        # install the needed ones and an exception is raised when one tries to
        # use a component with a dependency without having it installed first.
        'BeautifulSoup',
        'requests',
        'unidecode',
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
