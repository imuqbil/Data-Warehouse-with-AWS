import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    
    """
    Loads JSON source data (log & song data) from S3 and insert into staging_events and staging_songs tables
    Parameters:
    cur: cursory to the connected database 
    conn: connection to the Postgres database 

    """
    for query in copy_table_queries:  
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
       
    """
    Inserts data from the staging tables into the analytical tables 
    Parameters:
    cur: cursory to the connected database 
    conn: connection to the Postgres database 

    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    """
    Connects to the database and calls the load and inseret functions 

    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()