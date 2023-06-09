import configparser
import psycopg2
from sql_queries import select_number_rows

def select_number_row_of_table(cur, conn):
    """
    Gets the number of rows each table
    """
    for query in select_number_rows:
        print('\n'.join(('', 'Running:', query)))
        cur.execute(query)
        results = cur.fetchone()
        for row in results:
            print(row)

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    select_number_row_of_table(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
