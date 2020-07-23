import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    
    """
    Drop any existing tables from the database (sparkifydb)
    Parameters:
    cur : cursory to the connected database to execute the SQL statements
    conn :(psycopg2) connection to the Postgres database 
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    
    """
    Create tables into the database
    Parameters:
    cur : cursory to the connected database to execute the SQL statements
    conn :(psycopg2) connection to the Postgres database 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    """This function connects to Redshift, create a new database (sparkifydb),
        drop any existing table, create new tables, and close the database connection.
    Parameters of the configuration file (dwh.cfg):
    host: Redshift cluster endpoint address
    DB_NAME:   database name
    DB_USER:     database username 
    DB_PASSWORD: database password 
    DB_PORT:     database port of connection

    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()