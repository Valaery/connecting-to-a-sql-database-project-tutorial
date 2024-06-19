import os
import unittest
import sqlalchemy
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from dotenv import load_dotenv

class TestDatabaseSetup(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Load the .env file variables
        load_dotenv()
        
        # Connect to the database
        connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
        cls.engine = sqlalchemy.create_engine(connection_string)

        # Paths to the SQL scripts
        cls.create_script = './sql/create.sql'
        cls.insert_script = './sql/insert.sql'
        cls.drop_script = './sql/drop.sql'

    def execute_sql(self, filepath):
        try:
            with self.engine.connect() as connection:
                with open(filepath, 'r') as file:
                    sql_script = file.read()
                    connection.execute(sql_script)
        except OperationalError as oe:
            self.fail(f"Operational error: {oe}")
        except SQLAlchemyError as se:
            self.fail(f"SQLAlchemy error: {se}")
        except Exception as e:
            self.fail(f"Unexpected error: {e}")

    def test_create_tables(self):
        try:
            self.execute_sql(self.create_script)
        except Exception as e:
            self.fail(f"Failed to create tables: {e}")

    def test_insert_data(self):
        try:
            self.execute_sql(self.insert_script)
        except Exception as e:
            self.fail(f"Failed to insert data: {e}")

    def test_query_data(self):
        try:
            connection = self.engine.raw_connection()
            try:
                df_books = pd.read_sql("SELECT * FROM books", con=connection)
                df_authors = pd.read_sql("SELECT * FROM authors", con=connection)
                df_publishers = pd.read_sql("SELECT * FROM publishers", con=connection)
                df_book_authors = pd.read_sql("SELECT * FROM book_authors", con=connection)

                # Print dataframes for debugging
                print("Books DataFrame:")
                print(df_books)
                print("Authors DataFrame:")
                print(df_authors)
                print("Publishers DataFrame:")
                print(df_publishers)
                print("Book Authors DataFrame:")
                print(df_book_authors)

                self.assertFalse(df_books.empty, "The books table is empty.")
                self.assertFalse(df_authors.empty, "The authors table is empty.")
                self.assertFalse(df_publishers.empty, "The publishers table is empty.")
                self.assertFalse(df_book_authors.empty, "The book_authors table is empty.")
            finally:
                connection.close()
        except Exception as e:
            self.fail(f"Failed to query data: {e}")

    @classmethod
    def tearDownClass(cls):
        try:
            with cls.engine.connect() as connection:
                with open(cls.drop_script, 'r') as file:
                    sql_script = file.read()
                    connection.execute(sql_script)
        except Exception as e:
            print(f"Failed to drop tables: {e}")

if __name__ == '__main__':
    unittest.main()
