from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor 


'''Instantiate class instances'''
db_connector = DatabaseConnector()
data_cleaner = DataCleaning()
data_extractor = DataExtractor()

'''Use the class instances to create the objects you need'''
creds = db_connector.read_db_creds(yaml_file = 'db_creds.yaml')
local_creds = db_connector.read_db_creds(yaml_file= 'local_db_creds.yaml')
engine = db_connector.init_db_engine(creds)
local_engine = db_connector.init_db_engine(local_creds)

'''Use engine to extract data from RDS'''
user_df = data_extractor.read_rds_table(engine, 'legacy_users')

'''Clean user data'''
clean_user_df = data_cleaner.clean_user_data(user_df)

'''Push data to database'''
db_connector.upload_to_db(clean_user_df, 'dim_users', local_engine)

'''Extract, clean, and upload card data from pdf'''
card_df = data_extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
clean_card_df = data_cleaner.clean_card_data(card_df)
db_connector.upload_to_db(clean_card_df, 'dim_card_details', local_engine)

'''Extract, clean, and upload store data using API'''
return_number_of_stores_api = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
key = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
num_stores = data_extractor.list_number_of_stores(return_number_of_stores_api, key)
store_df = data_extractor.retrieve_stores_data(num_stores, key)
clean_store_df = data_cleaner.clean_store_data(store_df)
db_connector.upload_to_db(clean_store_df, 'dim_store_details', local_engine)

'''Extract, clean, and upload product data using AWS S3'''
product_df = data_extractor.extract_from_s3('data-handling-public','products.csv', 
                                             '/Users/malfa/Documents/multinational_retail_data/products.csv')