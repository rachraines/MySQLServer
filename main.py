import mysql.connector
from mysql.connector import Error
#import pandas as pd

# Configuration
HOST = "localhost"
USER = "rachraines"
PASSWORD = "992277rf!"
DATABASE = "school"

# SQL Queries
#CREATE_DATABASE_QUERY = f"CREATE DATABASE IF NOT EXISTS {DATABASE}"


# Connect to MySQL server (no database specified yet).
def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host = host_name,
            user = user_name,
            passwd = user_password
        )
        print("MySQL Server connection successful")
    except Error as err:
        print(f"Error: '{err}'")
    return connection

# Connect directly to the specified database.
def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host = host_name,
            user = user_name,
            passwd = user_password,
            database = db_name
        )
        print(f"MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")
    return connection

# Creates a database
def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print(f"Database: '{DATABASE}' created successfully")
    except Error as err:
        print(f"Error: '{err}'")

# Execute query
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Error as err:
        print(f"Error: '{err}'")

# Creating tables
create_teacher_table = """
CREATE TABLE IF NOT EXISTS teacher (
    teacher_id INT PRIMARY KEY,
    first_name VARCHAR(40) NOT NULL,
    last_name VARCHAR(40) NOT NULL,
    language_1 VARCHAR(3) NOT NULL,
    language_2 VARCHAR(3),
    dob DATE,
    tax_id INT UNIQUE,
    phone_no VARCHAR(20)
);
"""

create_client_table = """
CREATE TABLE client(
    client_id INT PRIMARY KEY,
    client_name VARCHAR(40) NOT NULL,
    address VARCHAR(60) NOT NULL,
    inddustry VARCHAR(20)
    );
    """

create_participant_table = """
CREATE TABLE participant(
participant_id INT PRIMARY KEY,
first_name VARCHAR(40) NOT NULL,
last_name VARCHAR(40) NOT NULL,
phone_no VARCHAR(20),
client INT
);
"""

create_course_table = """
CREATE TABLE course(
course_id INT PRIMARY KEY,
course_name VARCHAR(40) NOT NULL,
language VARCHAR(3) NOT NULL,
level VARCHAR(2),
course_length_weeks INT,
start_date DATE,
in_school BOOLEAN,
teacher INT,
client INT
);
"""

# Add many-to-many relationship table bw participant and client
alter_participant = """
ALTER TABLE participant
ADD FOREIGN KEY(client)
REFERENCES client(client_id)
ON DELETE SET NULL;
"""

# Add many-to-many relationship bw course and teacher
alter_course = """
ALTER TABLE course
ADD FOREIGN KEY(teacher)
REFERENCES teacher(teacher_id)
ON DELETE SET NULL;
"""

# Add many-to-many relationship bw course and client
alter_course_again = """
ALTER TABLE course
ADD FOREIGN KEY(client)
REFERENCES client(client_id)
ON DELETE SET NULL;
"""

create_takescourse_table = """
CREATE TABLE takes_course(
    participant_id INT,
    course_id INT,
    PRIMARY KEY(participant_id, course_id),
    FOREIGN KEY(participant_id) REFERENCES participant(participant_id) ON DELETE CASCADE,
    FOREIGN KEY(course_id) REFERENCES course(course_id) ON DELETE CASCADE
);
"""


def main():
    # Connect to MySql server (no DB yet)
    connection = create_server_connection(HOST, USER, PASSWORD)
    
    # Create the database if not already present
    create_database_query = "CREATE DATABASE school"
    create_database(connection, create_database_query)

    # Connect to the new database
    db_connection = create_db_connection(HOST, USER, PASSWORD, DATABASE)
    if db_connection is None:
        return
    
    # Create tables
    execute_query(db_connection, create_teacher_table)
    execute_query(db_connection, create_client_table)
    execute_query(db_connection, create_participant_table)
    execute_query(db_connection, create_course_table)
    execute_query(db_connection, alter_participant)
    execute_query(db_connection, alter_course)
    execute_query(db_connection, alter_course_again)
    execute_query(db_connection, create_takescourse_table)

    db_connection.close()
    print("All setup completed successfully.")


if __name__ == "__main__":
    main()