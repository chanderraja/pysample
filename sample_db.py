__author__ = 'chander.raja@gmail.com'

import mysql.connector

from mysql.connector import errorcode
from mysql.connector.constants import ClientFlag

config = {
    'user': 'admin',
    'password': '$aFes4WRT',
    'host': '127.0.0.1',
    'database': 'tuna',
    'raise_on_warnings': True,
    'client_flags': [ClientFlag.LOCAL_FILES],
    'autocommit': True
}


def open():
    print("Connecting to DB...")
    try:
        cnx = mysql.connector.connect(**config)

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Database access denied")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
        sys.exit(1)
    else:
        print("Connected")
        return cnx


def close(cnx):
    print("Closing DB...")
    cnx.close()
