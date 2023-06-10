"""Get the number of rows each table."""

import configparser
import psycopg2
from sql_queries import select_number_rows


def select_number_row_of_table(cur, conn):
    """
    Get the number of rows each table.

            Parameters:
                    conn (connection): instance of connection class
                    cur (cursor): instance of cursor class

            Returns:
                    none
    """
    for query in select_number_rows:
        print('\n'.join(('', 'Running:', query)))
        cur.execute(query)
        results = cur.fetchone()
        for row in results:
            print(row)


def main():
    """Count the number of rows each table."""
    # Config
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connection to database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(*config['CLUSTER'].values()))

    # Create cusor object
    cur = conn.cursor()

    # ETL queries
    select_number_row_of_table(cur, conn)

    # Close connection
    conn.close()


if __name__ == "__main__":
    main()
