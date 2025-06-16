from execute_query import *
from create_db_connection import *

create_teacher_table = """
CREATE TABLE teacher(
teacher_id INT PRIMARY KEY,
first_name VARCHAR(40) NOT NULL,
last_name VARCHAR(40) NOT NULL,
language_1 VARCHAR(3) NOT NULL,
language_2 VARCHAR (3),
dob DATE,
tax_id INT UNIQUE,
phone_no VARCHAR(20)
);
"""

connection = create_db_connection("localhost", "root", pw, db)
execute_query(connection, create_teacher_table)