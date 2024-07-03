import pandas as pd
import requests
from sqlalchemy import text
from tabula.io import read_pdf
import boto3

class DataExtractor:

    def __init__(self) -> None:
        pass
    
    def read_rds_table(self, engine, table_name):
        db_engine = engine
        with db_engine.connect() as connection:
            result = connection.execute(text(f"SELECT * FROM {table_name}"))
            result = pd.DataFrame(result)
            return result
        
    def retrieve_pdf_data(self, url):
        pdf_data = pd.concat(read_pdf(url, pages='all'))
        return pdf_data
    
    def list_number_of_stores(self, url, headers):
        number_of_stores = requests.get(url, headers=headers)
        num_stores = number_of_stores.json()['number_stores']
        return num_stores
    
    def retrieve_stores_data(self, num_stores, headers):
        df_list = []
        for i in range(num_stores):
            stores_url = (f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{i}") 
            store_data = requests.get(stores_url, headers=headers)
            temp_df = pd.json_normalize(store_data.json())
            df_list.append(temp_df)
        stores_df = pd.concat(df_list)
        return stores_df
    
    def extract_from_s3(self, bucket_name, file_name, file_destination):
        s3 = boto3.client('s3')
        data = pd.DataFrame(s3.download_file(bucket_name, file_name, file_destination))
        return data


if __name__ == '__main__':
    pass