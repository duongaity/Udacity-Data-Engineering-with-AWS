"""Load data from S3 into staging tables and insert to table."""

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from S3 into staging tables.

            Parameters:
                    conn (connection): instance of connection class
                    cur (cursor): instance of cursor class

            Returns:
                    none
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Create your fact and dimension tables.

            Parameters:
                    conn (connection): instance of connection class
                    cur (cursor): instance of cursor class

            Returns:
                    none
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Load data from S3 into staging tables and insert tables."""
    # Config
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connection to database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(*config['CLUSTER'].values()))

    # Create cusor object
    cur = conn.cursor()

    # ETL queries
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    # Close connection
    conn.close()


if __name__ == "__main__":
    main()
