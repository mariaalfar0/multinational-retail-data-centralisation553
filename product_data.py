from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor 
import pandas as pd


'''Instantiate class instances'''
db_connector = DatabaseConnector()
data_cleaner = DataCleaning()
data_extractor = DataExtractor()

'''Extract, clean, and upload product data using AWS S3'''
data_extractor.extract_from_s3('data-handling-public','products.csv','/Users/malfa/Documents/multinational_retail_data/products.csv')
product_data = pd.read_csv('products.csv')
converted_product_data = data_cleaner.convert_products_weight(product_data)
clean_product_data = data_cleaner.clean_products_data(converted_product_data)

print(clean_product_data.uuid_valid)
print(clean_product_data.info())


