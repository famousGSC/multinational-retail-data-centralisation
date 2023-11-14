import pandas as pd
import yaml
from sqlalchemy import create_engine

class DatabaseConnector:
    def __init__(self):
        pass
  
    def read_db_creds(self):
        # Initialises a context manager to read in the credentials YAML file
        with open('db_creds.yaml', 'r') as creds:
            data_loaded = yaml.safe_load(creds)
        return data_loaded 

    def init_db_engine(self, data_loaded):
        engine = create_engine(f"""
            {'postgresql'}+
            {'psycopg2'}://
            {data_loaded['RDS_USER']}:
            {data_loaded['RDS_PASSWORD']}@
            {data_loaded['RDS_HOST']}:
            {data_loaded['RDS_PORT']}/
            {data_loaded['RDS_DATABASE']}""")
        
        return engine


my_instance = DatabaseConnector()
my_creds = my_instance.read_db_creds()
print(my_creds['RDS_DATABASE'])