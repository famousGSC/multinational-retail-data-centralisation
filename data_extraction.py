from database_utils import DatabaseConnector
import pandas as pd

class DataExtractor:
    def __init__(self):
        pass
    def read_rds_table(self, connector, table):
            my_creds = connector.read_db_creds()
            my_engine = connector.init_db_engine(my_creds)
            table_to_read = pd.read_sql_table(table, my_engine)
            return table_to_read


    def extract_to_dataframe(self, engine):
        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn: # Using a context manager negates the need to manually close the connection
            result = conn.execute(text("SELECT * FROM actor"))
            for row in result:
                print(row)


my_connector = DatabaseConnector()

my_df = DataExtractor.read_rds_table(my_connector, 'legacy_users')