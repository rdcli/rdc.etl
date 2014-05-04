Database
========

Database extracts, loads and joins are implemented in the rdc.etl.extra.db package. It's considered as an
"addon", because no work has been made yet on "connection management" in the core package.

You need sqlalchemy, below is an example.

.. code-block:: python

    # -*- coding: utf-8 -*-

    import datetime
    import sqlalchemy

    from rdc.etl.extra.db import DatabaseExtract, DatabaseLoad
    from rdc.etl.extra.util import TransformBuilder
    from rdc.etl.job import Job
    from rdc.etl.status.console import ConsoleStatus
    from rdc.etl.transform import Transform


    DB_CONFIG = {
        'user': 'root',
        'pass': '',
        'name': 'my_database',
        'host': 'localhost',
    }
    TABLE_NAME = 'products'


    # Create SQLAlchemy engine
    db_engine = sqlalchemy.create_engine('mysql://{user}:{pass}@{host}/{name}'.format(**DB_CONFIG))


    # Extract : use a SQL query
    t1 = DatabaseExtract(
        db_engine,
        '''
            SELECT *
            FROM {table_name} t
            WHERE MOD(t.id, 100) > 98
        '''.format(table_name=TABLE_NAME)
    )


    # Transform : Update a timestamp
    @TransformBuilder(Transform)
    def UpdateChangeTimestamp(hash, channel):
        hash['updated_at'] = datetime.datetime.now()
        yield hash


    t2 = UpdateChangeTimestamp()


    # Load : same table as input (by choice)
    t3 = DatabaseLoad(
        db_engine,
        TABLE_NAME,
        discriminant=('id', ),  # This is default behavior, but the selection criteria can be based on any field
        # combination as long as a select on those keys returns only ONE result line.
        updated_at_field=None,  # Avoid default updated_at behavior as we reimplemented it manually.
    )

    # Job creation
    job = Job(profile=True)
    job.add_chain(t1, t2, t3)
    job.status.append(ConsoleStatus())

    if __name__ == '__main__':
        job()


