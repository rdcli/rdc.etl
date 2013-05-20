from setuptools import setup, find_packages
import sys, os

with open('VERSION') as f:
    version = f.read()

setup(name='rdc.etl',
      version=version,
      description="ETL in pure python",
      long_description="""Extract Transform Load, using python""",
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

      ], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='',
      author='SARL Romain Dorgueil Conseil',
      author_email='romain@dorgueil.net',
      url='https://rdc.li/etl/',
      license='Proprietary.',
      packages=find_packages('src', exclude=['ez_setup', 'examples', 'tests']),
      package_dir = {'': 'src'},
      include_package_data=True,
      zip_safe=False,
      install_requires=[
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
