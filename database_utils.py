import pandas as pd
import yaml
from sqlalchemy import create_engine
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
        #for table_name in inspector.get_table_names():
        #    for column in inspector.get_columns(table_name):
        #        print("Column: %s" % column['name'])
        return inspector
        

my_instance = DatabaseConnector()
my_creds = my_instance.read_db_creds()
my_engine = my_instance.init_db_engine(my_creds)
my_tables = my_instance.list_db_tables(my_engine)
print(my_tables)