from database_utils import DatabaseConnector

class DataExtractor:
    def __init__(self):


    def first_method(self):
        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn: # Using a context manager negates the need to manually close the connection
            result = conn.execute(text("SELECT * FROM actor"))
            for row in result:
            print(row)