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

      Create a harness.

      >>> from rdc.etl.harness.threaded import ThreadedHarness as Harness
      >>> harness = Harness()

      Create some data transformations.

      >>> from rdc.etl.transform.extract import Extract
      >>> extract = Extract(stream_data=({'foo': 'bar'}, {'foo': 'baz'}))

      >>> from rdc.etl.transform.simple import SimpleTransform
      >>> transform = SimpleTransform()
      >>> transform.add('foo').filter('upper')

      >>> from rdc.etl.transform.util import Log
      >>> load = Log()

      Tie everything together.

      >>> harness.add_chain(extract, transform, load)

      Run.

      >>> harness()

      This is a work in progress, the 1.0 API may change until 1.0 is released.

      """,
      classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Topic :: Database',
        'Topic :: Utilities',
        'Topic :: Text Processing :: Filters',
      ],
      keywords='ETL Data-Integration',
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
        # todo: most of the deps are optional and depends on what we wanna
        # actually use, make it so nothing is required, but still travis
        # install the needed ones and an exception is raised when one tries to
        # use a component with a dependency without having it installed first.
        'BeautifulSoup',
        'requests',
        'unidecode',
        'werkzeug', # how to get cached_property without this ? btw, the webapp2 version is better
        'sqlalchemy',
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
