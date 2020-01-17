import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    ''' Load or copy log and song data from the S3 buckets into the staging tables created in the redshift clusters'''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    ''' Insert or load specified columns data from the staging tables into the analytics tables in the redshift clusters'''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    ''' main() control the execution flow, First, by setting up connection to the Redshift DWH clusters based on some properties,
    then, data is copied from S3 to staging tables and finally, 
    data is inserted from specified columns in the staging tables into the analytics table, 
    all of this occuring within the Redshift cluster'''
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()