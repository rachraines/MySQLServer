from create_server_connection import connection
import mysql.connector
from mysql.connector import Error
import pandas as pd

def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")

create_database_query = "CREATE DATABASE school"
create_database(connection, create_database_query)