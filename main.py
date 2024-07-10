from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor 
import pandas as pd


'''Instantiate class instances'''
db_connector = DatabaseConnector()
data_cleaner = DataCleaning()
data_extractor = DataExtractor()

'''Use the class instances to create the objects you need'''
creds = db_connector.read_db_creds(yaml_file = 'db_creds.yaml')
local_creds = db_connector.read_db_creds(yaml_file= 'local_db_creds.yaml')
engine = db_connector.init_db_engine(creds)
local_engine = db_connector.init_db_engine(local_creds)

'''Use engine to extract, clean, and push user data from RDS'''
user_data = data_extractor.read_rds_table(engine, 'legacy_users')
clean_user_data = data_cleaner.clean_user_data(user_data)
db_connector.upload_to_db(clean_user_data, 'dim_users', local_engine)

'''Extract, clean, and upload card data from pdf'''
card_data = data_extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
clean_card_data = data_cleaner.clean_card_data(card_data)
db_connector.upload_to_db(clean_card_data, 'dim_card_details', local_engine)

'''Extract, clean, and upload store data using API'''
return_number_of_stores_api = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
key = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
num_stores = data_extractor.list_number_of_stores(return_number_of_stores_api, key)
stores_data = data_extractor.retrieve_stores_data(num_stores, key)
clean_stores_data = data_cleaner.clean_store_data(stores_data)
db_connector.upload_to_db(clean_stores_data, 'dim_store_details', local_engine)

'''Extract, clean, and upload product data using AWS S3'''
data_extractor.extract_from_s3('data-handling-public','products.csv','/Users/malfa/Documents/multinational_retail_data/products.csv')
product_data = pd.read_csv('products.csv')
converted_product_data = data_cleaner.convert_products_weight(product_data)
clean_product_data = data_cleaner.clean_products_data(converted_product_data)
db_connector.upload_to_db(clean_product_data, 'dim_product_details', local_engine)

'''Use engine to extract, clean, and push order data from RDS'''
orders_data = data_extractor.read_rds_table(engine, 'orders_table')
clean_orders_data = data_cleaner.clean_orders_data(orders_data)
db_connector.upload_to_db(clean_orders_data, 'orders_table', local_engine)

'''Extract, clean, and upload product data using AWS S3'''
data_extractor.extract_from_s3('data-handling-public','date_details.json','/Users/malfa/Documents/multinational_retail_data/date_details.json')
date_data = pd.read_json('date_details.json')
clean_date_data = data_cleaner.clean_date_data(date_data)
db_connector.upload_to_db(clean_date_data, 'dim_date_details', local_engine)


