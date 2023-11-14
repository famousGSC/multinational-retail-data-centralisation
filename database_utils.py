import pandas as pd
import yaml

class DatabaseConnector:
    def __init__(self):
        pass
  
    def read_db_creds(self):
        # Initialises a context manager to read in the credentials YAML file
        with open('db_creds.yaml', 'r') as creds:
            data_loaded = yaml.safe_load(creds)
        return data_loaded 

    def init_db_engine(self, data_loaded):
        ## Initialise and return a SQLalchemy engine
        pass
    

my_instance=DatabaseConnector()
print(my_instance.read_db_creds())