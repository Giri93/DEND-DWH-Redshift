import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description: This function can be used to call the functions to process song/log json files 
    and load the staging tables in Redshift
    
    Arguments:
        cur: the cursor object. 
        conn: the connection object 
    
    Returns:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """
    Description: This function can be used to call the functions to process data 
    from staging tables to dimension, Fact tables in Redshift
    
    Arguments:
        cur: the cursor object. 
        conn: the connection object 
    
    Returns:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    print("Staging Tables Loaded")
    
    insert_tables(cur, conn)
    print("Dimension & Fact Tables Loaded")

    conn.close()
    print("Connection Closed")


if __name__ == "__main__":
    main()