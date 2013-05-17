from setuptools import setup, find_packages
import sys, os

version = '0.0'

setup(name='rdc.etl',
      version=version,
      description="",
      long_description="""\
""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='',
      author='Romain Dorgueil',
      author_email='romain@dorgueil.net',
      url='https://rdc.li/etl/',
      license='GPL',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      package_dir = {'': 'src'},
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'processing',
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
