from setuptools import setup, find_packages

with open('VERSION') as f:
    version = f.read()

setup(name='rdc.etl',
      version=version,
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
      license='Apache License, Version 2.0',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      package_dir = {'': 'src'},
      include_package_data=True,
      zip_safe=False,
      install_requires=[
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
