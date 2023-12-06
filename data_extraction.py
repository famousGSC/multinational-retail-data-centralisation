from database_utils import DatabaseConnector
import pandas as pd
import tabula
import requests
#import boto3

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


    def list_number_of_stores(self, header, number_of_stores_endpoint):
        try:
            response = requests.get(number_of_stores_endpoint, headers=header)
            if response.status_code == 200:
                number_of_stores = response.json().get('number_of_stores')
                return number_of_stores
            else:
                print(f"Failed to retrieve number of stores. Status code: {response.status_code}")
        except requests.RequestException as e:
            print(f"Error: {e}")
            return None
        

    def retrieve_stores_data(self, store_endpoint):
        try:
            number_of_stores = self.list_number_of_stores(number_of_stores_endpoint)
            stores_data = []

            if number_of_stores is not None:
                for store_number in range(1, number_of_stores + 1):
                    store_url = store_endpoint.format(store_number)
                    response = requests.get(store_url, headers=self.header)
                    if response.status_code == 200:
                        store_details = response.json()
                        stores_data.append(store_details)
                    else:
                        print(f"Failed to retrieve store {store_number}. Status code: {response.status_code}")

                if stores_data:
                    # Convert the list of store details into a Pandas DataFrame
                    df = pd.DataFrame(stores_data)
                    return df
                else:
                    print("No store data found.")
                    return None
        except requests.RequestException as e:
            print(f"Error: {e}")
            return None

# Define the API key and endpoints
header = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
retrieve_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{}'

# Create an instance of DataExtractor and retrieve the number of stores
data_extractor = DataExtractor()
number_of_stores = data_extractor.list_number_of_stores(number_of_stores_endpoint, header)
stores_dataframe = data_extractor.retrieve_stores_data(retrieve_store_endpoint)

if stores_dataframe is not None:
    print("Stores data successfully retrieved!")
    print(stores_dataframe.head())  # Display the first few rows of the DataFrame





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