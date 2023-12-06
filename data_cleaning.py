import pandas as pd

class DataCleaning:
    def __init__(self):
        pass

    def clean_user_data(self, df):
        if df.isnull().values.any() == True:
            print(f'Replacing {df.isnull().sum()} null values')
            df = df.fillna('')
        else:
            print('No null values')
        
    def clean_store_data(self, stores_df):
        if df.isnull().values.any() == True:
            print(f'Replacing {df.isnull().sum()} null values')
            df = df.fillna('')
        else:
            print('No null values')

    def clean_orders_data(self, orders_df):
        return orders_df.drop(columns=['first_name', 'last_name', '1'])
    