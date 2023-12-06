import pandas as pd
import yaml
from sqlalchemy import create_engine, MetaData, Table, Column, String, BigInteger, SmallInteger
from sqlalchemy import inspect
import psycopg2

class DatabaseConnector:
    def __init__(self):
        pass
  
    def read_db_creds(self):
        # Initialises a context manager to read in the credentials YAML file
        with open('db_creds.yaml', 'r') as creds:
            data_loaded = yaml.safe_load(creds)
        return data_loaded 

    def init_db_engine(self, data_loaded):
        engine = create_engine(f"postgresql+psycopg2://"
                               f"{data_loaded['RDS_USER']}:"
                               f"{data_loaded['RDS_PASSWORD']}@"
                               f"{data_loaded['RDS_HOST']}:"
                               f"{data_loaded['RDS_PORT']}/"
                               f"{data_loaded['RDS_DATABASE']}")
        return engine

    def list_db_tables(self, engine):
        inspector = inspect(engine)
        print(inspector.get_table_names())
        

    def upload_to_db(self, df, tablename):
        df.to_sql(name=tablename, con=my_engine)
        

my_connector = DatabaseConnector()
my_creds = my_connector.read_db_creds()
my_engine = my_connector.init_db_engine(my_creds)
my_connector.list_db_tables(my_engine)
