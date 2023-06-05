"""For manger managing PostgresSql and RedShift"""
import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from src.common.utils.config import Config
from src.common.utils.logger import logging

class RedshiftConnector:
    """
    Initializes the RedshiftConnector with connection details.

    Example:
        - connector = RedshiftConnector(key_config='redshift')
        - connector.execute_query('SELECT * FROM my_table')
        - connector.disconnect()
    """

    def __init__(self, key_config: str="postgresql"):
        """
        Initializes the RedshiftConnector with connection details.

        Args:
            key_config (str): The key for getting configuration details from the 'config.yml' file.
        """
        config = Config.get_config_yml(key_config)
        try:
            self.conn = psycopg2.connect(os.getenv(config))
            self.cursor = self.conn.cursor()
            logging.info("Connected to the database successfully.")
        except (psycopg2.Error) as error:
            logging.error(f"Error connecting to the database: {str(error)}")
            raise Exception(f"Error connecting to the database: {str(error)}") from error

    def disconnect(self):
        """
        Close the connection to the database.
        """
        if self.conn is not None:
            self.cursor.close()
            self.conn.close()
            logging.info("Disconnected from the database.")


    def load_data_from_df(self, dataframe: pd.DataFrame, table_name: str) -> None:
        """
        Load data from a pandas DataFrame into a database table in Postgres.

        Args:
            dataframe (pd.DataFrame): The DataFrame containing the data to load.
            table_name (str): The name of the table to load the data into.

        Returns:
            None

        Raises:
            ValueError: If the DataFrame is empty.

        Example:
            connector = RedshiftConnector()
            data = pd.DataFrame({'id': [1, 2, 3], 'name': ['John', 'Jane', 'Alice']})
            connector.load_data_from_df(data, 'users')
        """
        try:
            # Generate the placeholders for the VALUES statement
            value_placeholders = ','.join(['%s'] * len(dataframe.columns))
            columns = ','.join(dataframe.columns)
            # Prepare the SQL query with the table name and column names
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({value_placeholders})"
            # Convert the DataFrame to a list of tuples
            data = [tuple(row) for row in dataframe.values]
            self.cursor.executemany(query, data)
            # Commit the changes and close the cursor
            self.conn.commit()
            logging.info(f"Data loaded into table '{table_name}' successfully.")
        except (psycopg2.Error) as error:
            logging.error(f"Error : {str(error)}")
            raise Exception(f"Error : {str(error)}") from error

class MySQLConnector:
    """For connection to MySQL using SQLAlchemy"""

    def __init__(self, key_config: str='myslq'):
        config = Config.get_config_yml(key_config)
        try:
            self.engine = create_engine(os.getenv(config))
            self.session = sessionmaker(bind=self.engine)
            logging.info("Connected to MySQL")
        except Exception as error:
            logging.error(f"Error connecting to MySQL: {error}")
            raise error

    def execute_query(self, query):
        """Executes an SQL query on the MySQL database."""
        try:
            session = self.session()
            session.execute(text(query))
            session.commit()
            logging.info("Query executed successfully")
        except Exception as error:
            logging.error(f"Error executing query: {error}")
            session.rollback()
            raise error
        finally:
            session.close()

    def insert_dataframe(self, table_name, dataframe):
        """Inserts data from a DataFrame into a MySQL table."""
        try:
            dataframe.to_sql(table_name, self.engine, if_exists='append', index=False)
            logging.info("Data inserted successfully")
        except Exception as error:
            logging.error(f"Error inserting data: {error}")
            raise error

    def close(self):
        """Closes the connection to the MySQL database."""
        try:
            self.engine.dispose()
            logging.info("Connection closed")
        except Exception as error:
            logging.error(f"Error closing connection: {error}")
            raise error


# class MySQLConnector:
#     """For connection to MySQL"""

#     def __init__(self):
#         config = Config.get_config_yml("mysql")
#         Config.load_config()
#         logging = Logger(__name__)
#         try:
#             self.connection = mysql.connector.connect(
#                 host=os.getenv(config['mysql_host']),
#                 port=os.getenv(config['mysql_port']),
#                 database=os.getenv(config['mysql_db']),
#                 user=os.getenv(config['mysql_user']),
#                 password=os.getenv(config['mysql_password'])
#             )
#             logging.info("Connected to MySQL")
#         except mysql.connector.Error as error:
#             logging.error(f"Error connecting to MySQL: {error}")
#             raise error

#     def execute_query(self, query):
#         """Executes an SQL query on the MySQL database."""
#         try:
#             cursor = self.connection.cursor()
#             cursor.execute(query)
#             self.connection.commit()
#             logging.info("Query executed successfully")
#         except mysql.connector.Error as error:
#             logging.error(f"Error executing query: {error}")
#             raise error

#     def insert_dataframe(self, table_name, dataframe):
#         """Inserts data from a DataFrame into a MySQL table."""
#         try:
#             cursor = self.connection.cursor()
#             columns = ','.join(dataframe.columns)
#             values = ','.join(['%s'] * len(dataframe.columns))
#             query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
#             records = dataframe.to_records(index=False).tolist()
#             cursor.executemany(query, records)
#             self.connection.commit()
#             logging.info("Data inserted successfully")
#         except mysql.connector.Error as error:
#             logging.error(f"Error inserting data: {error}")
#             raise error

#     def close(self):
#         """Closes the connection to the MySQL database."""
#         try:
#             if self.connection:
#                 self.connection.close()
#                 logging.info("Connection closed")
#         except mysql.connector.Error as error:
#             logging.error(f"Error closing connection: {error}")
#             raise error
