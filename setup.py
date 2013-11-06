import os, sys
from setuptools import setup, find_packages

# Force our version to be used, don't know what is best practice here.
sys.path.insert(0, os.path.join(os.getcwd(), 'src'))
from rdc.etl import __version__

setup(name='rdc.etl',
      version=__version__,
      description="Extract Transform Load toolkit for python",
      long_description="""Data integration toolkit, using standard ETL patterns.
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
