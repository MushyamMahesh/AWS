#!/usr/bin/env python3

import argparse
import psycopg2

from db_config import config

conn = None  # global variable


def set_connection(section):
    """
    Gets connection to PostgreSQL database instance
    :param section: section of database configuration file to use
    :return: db connection
    """

    try:
        params = config(filename='database.ini', section=section)
        global conn
        conn = psycopg2.connect(**params)
        print('Connection to database created')
    except (Exception, psycopg2.DatabaseError) as err:
        print(set_connection.__name__, err)
        exit(1)


def db_info():
    """
    Gets database information
    """

    try:
        global conn
        with conn:
            with conn.cursor() as curs:
                curs.execute('SELECT version()')
                db_version = curs.fetchone()
                return db_version
    except (psycopg2.OperationalError, psycopg2.DatabaseError) as err:
        print(db_info.__name__, err)
        close_conn()
        exit(1)


def deploy_scripts():
    """
    Deploy Sql Scripts
    """

    try:
        global conn
        with conn:
            with conn.cursor() as curs:
                try:
                    curs.execute("begin; SAVEPOINT Test12345;")
                    file = open("../Workflow_Scripts/Deploy_files.txt","r")
                    content = file.read()
                    print(content)
                    file.close()
                    files = content.split('\n')
                    for file in files:
                        if file != '':
                            print(file)
                            curs.execute(open(file, "r").read())
                    #curs.execute(open("../sql-scripts/data.sql", "r").read())
                    raise NameError('Test Error')
                    conn.commit()
                    print('SQL scripts executed')
                except NameError as err:
                    curs.execute('ROLLBACK TO SAVEPOINT Test12345; end;')
                    print('error raised intentionally')
                    close_conn()
                    exit(1)
    except (psycopg2.OperationalError, psycopg2.DatabaseError, FileNotFoundError) as err:
        print(deploy_scripts.__name__, err)
        close_conn()
        exit(1)
    except NameError as err:
        curs.execute('ROLLBACK TO SAVEPOINT Test12345;')
        print('error raised intentionally')
        close_conn()
        exit(1)



def close_conn():
    """
    Closes database connection
    """
    if conn is not None:
        conn.close()
        print('Database connection closed')


def parse_args():
    """
    Read in command-line parameters
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--instance", default="master", help="Database instance")

    return parser.parse_args()


def main():
    args = parse_args()

    set_connection(args.instance)
    print('Database info:', db_info())

    deploy_scripts()
    close_conn()


if __name__ == '__main__':
    main()
