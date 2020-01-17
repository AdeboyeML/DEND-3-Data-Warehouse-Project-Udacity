import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    ''' Delete the existing tables in order to create new ones'''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    ''' Create new tables that are specified in the create_table_queries '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    ''' main() control the execution flow here, First, setting up connection to the redshift clusters, 
    then dropping existing tables and further creating new ones.'''
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()