import os
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError, OperationalError
import pandas as pd
from dotenv import load_dotenv

# load the .env file variables
load_dotenv()

# 1) Connect to the database here using the SQLAlchemy's create_engine function
connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
engine = create_engine(connection_string)

# 2) Execute the SQL sentences to create your tables using the SQLAlchemy's execute function
# 3) Execute the SQL sentences to insert your data using the SQLAlchemy's execute function


def execute_sql(engine, filepath):
    try:
        with engine.connect() as connection:
            with open(filepath, 'r') as file:
                sql_script = file.read()
                connection.execute(sql_script)
    except Exception as e:
        print(f"An unexpected error occurred while reading data: {e}")

create_script = './sql/create.sql'
insert_script = './sql/insert.sql'
drop_script = './sql/drop.sql'


try:
    execute_sql(engine, create_script)
    execute_sql(engine, insert_script)
except Exception as e:
    print(f"An unexpected error occurred: {e}")
    execute_sql(engine, drop_script)

# 4) Use pandas to print one of the tables as dataframes using read_sql function
try:
    df = pd.read_sql("SELECT * FROM books", con=engine)
    print(df.head())
except Exception as e:
    print(f"An unexpected error occurred while reading data: {e}")