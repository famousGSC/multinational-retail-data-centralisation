from database_utils import DatabaseConnector
import pandas as pd
import tabula
import requests

class DataExtractor:
    def __init__(self):
        pass
    def read_rds_table(self, connector, table):
            my_creds = connector.read_db_creds()
            my_engine = connector.init_db_engine(my_creds)
            table_to_read = pd.read_sql_table(table, my_engine)
            return table_to_read

    def retrieve_pdf_data(self, link):
         return tabula.read_pdf(link, pages='all')
    
    def clean_card_data(self, df):
        if df.isnull().values.any() == True:
            print(f'Replacing {df.isnull().sum()} null values')
            df = df.fillna('')
        else:
            print('No null values')
        return df
    
    def list_number_of_stores(self, url, headers):
        response = requests.get(url, headers)
        if response.status_code == 200:
            number_of_stores = response.json()
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response Text: {response.text}")
        return number_of_stores

    def retrieve_stores_data(self, endpoint):
        response = requests.get(endpoint)
        if response.status_code == 200:
            data = response.json()
            stores_df = pd.DataFrame(data)
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response Text: {response.text}")
        return stores_df

# Instantiate connector
my_connector = DatabaseConnector()
# 
user_df = DataExtractor.read_rds_table(my_connector, 'legacy_users')
card_details_df = my_connector.retrieve_pdf.data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

# Upload card details
cleaned_card_dataframe = my_connector.clean_card_data(card_details_df)
my_connector.upload_to_db(cleaned_card_dataframe, 'dim_card_details')

# Extract store details
number_of_stores_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
headers = {
    "Content-Type": "application/json",
    "x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
}
endpoint =  "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
store_numbers = my_connector.list_number_of_stores(number_of_stores_url, headers)

store_details = my_connector.retrieve_stores_data(endpoint)

# Upload to db
my_connector.upload_to_db(store_details)