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

'''Clean up data'''
clean_user_df = data_cleaner.clean_user_data(user_df)

'''Push data to database'''
db_connector.upload_to_db(clean_user_df, 'dim_users', local_engine)