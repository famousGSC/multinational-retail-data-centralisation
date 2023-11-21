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



my_connector = DatabaseConnector()

my_df = DataExtractor.read_rds_table(my_connector, 'legacy_users')