import os, sys
from setuptools import setup, find_packages

# Force our version to be used, don't know what is best practice here.
sys.path.insert(0, os.path.join(os.getcwd(), 'src'))
from rdc.etl import __version__

with open("README.rst") as readme:
    long_description = readme.read()

setup(name='rdc.etl',
      version=__version__,
      description="Extract Transform Load (ETL) toolkit for python",
      long_description=long_description,
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
      url='http://etl.rdc.li/',
      download_url='https://github.com/rdconseil/etl/tarball/' + __version__,
      license='Apache License, Version 2.0',
      packages=find_packages('src', exclude=['ez_setup', 'example', 'test']),
      package_dir = {'': 'src'},
      include_package_data=True,
      zip_safe=False,
      install_requires=[
        'lxml',          # XML Processing
        'BeautifulSoup', 
        'requests',      # HTTP without headaches
        'unidecode',     # Transcoding
        'webob',         # Webapp2 dependency
        'webapp2',       # Basic web tools
        'sqlalchemy',    # DBAL
        'blessings',     # Readable console & tty detection
        'psutil',        # memory consumption
        'repoze.lru',    # lru_cache for python 2.6+
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
