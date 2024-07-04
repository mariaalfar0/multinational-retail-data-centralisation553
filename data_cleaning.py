import re
import numpy as np 
import pandas as pd
from datetime import datetime

class DataCleaning:
    # Methods to clean data from each of the data sources.
    def __init__(self) -> None:
        pass

    def clean_user_data(self, user_data):
        # Get rid of duplicates
        user_data = user_data.drop_duplicates()
        # Drop null values 
        user_data = user_data[user_data.first_name != 'NULL']    
        user_data = user_data.dropna()
        # Strip spaces etc. from start of strings
        name_cols = user_data.select_dtypes(object).columns
        user_data[name_cols] = user_data[name_cols].apply(lambda x: x.str.strip())
        # Fix typos in country codes and ensure that all data comes from GB, USA, DE
        user_data['country_code'] = user_data['country_code'].str.replace('GGB','GB')
        country_codes = ['GB', 'DE', 'US']
        user_data = user_data[user_data.country_code.isin(country_codes)]
        countries = ['United Kingdom', 'Germany', 'United States']
        user_data = user_data[user_data.country.isin(countries)]
        # Put dates into correct format
        date_cols = ['date_of_birth','join_date']
        for date_col in date_cols:
            user_data.loc[:,date_col] = user_data.loc[:,date_col].apply(pd.to_datetime, errors='coerce')
        # Remove escape characters
        user_data.loc[:,'address'] = user_data.loc[:,'address'].apply(lambda x:x.replace('\n', ','))
        return user_data
    
    def clean_card_data(self, card_data):
        # Reset indexing
        index = [row for row in range(0,len(card_data))] 
        card_data['index'] = index
        card_data = card_data.set_index(['index'])
        # Get rid of duplicates
        card_data = card_data.drop_duplicates()
        # Drop NULL values
        card_data = card_data[card_data.card_number != 'NULL']
        card_data = card_data.dropna()
        # Infer datatypes
        card_data['card_number'] = card_data['card_number'].astype('str')     
        card_data['card_provider'] = card_data['card_provider'].astype('str')
        # Drop wrong values
        card_data.loc[:,'card_number'] = card_data.loc[:,'card_number'].astype('str').apply(lambda x:x.replace('?',''))
        card_data = card_data[card_data['card_number'].str.isnumeric()]                                                                                       ###
        # Fix date formatting
        card_data.loc[:,'expiry_date'] = \
        card_data.loc[:,'expiry_date'].apply(pd.to_datetime, format='%m/%y')
        card_data.loc[:,'date_payment_confirmed'] = \
        card_data.loc[:,'date_payment_confirmed'].apply(pd.to_datetime,errors='coerce')
        return card_data
    
    def clean_store_data(self, stores_data):
        # Delete duplicate indexer column 
        stores_data = stores_data.iloc[:,1:]
        # Drop duplicates
        stores_data = stores_data.drop_duplicates()
        # Drop rows with wrong values
        stores_data = stores_data.loc[stores_data['country_code'].isin(['GB','US','DE'])]
        # Remove escape characters
        stores_data.loc[:,'address'] =  stores_data.loc[:,'address'].astype('str').apply(lambda x:x.replace('\n', ','))
        # Fix date formatting
        stores_data[['opening_date']] = \
        stores_data[['opening_date']].apply(pd.to_datetime,errors='coerce')
        # Fix continent misspellings
        stores_data['continent'] = stores_data['continent'].str.replace('eeEurope','Europe')
        stores_data['continent'] = stores_data['continent'].str.replace('eeAmerica','America')
        return stores_data 
    
    # Ensure correct format e.g. '0.77g.'
    def correct_format(self, value):
        if value[-1].isalnum() is False:
            wrong_char = value[-1]
            value= value.replace(wrong_char,'').strip()
        return value
    
    # Convert oz to kg
    def convert_oz(self,value):
        if 'oz' in value:
            value = value.replace('oz','')
            value = float(value) * 28.3495
        return value
    
    # Convert grams to kg
    def convert_grams(self,value):
        if value[-1] == 'g' and value[-2].isdigit() and value[:-2].isdigit()or value[-2:] == 'ml':
            value = value.replace('g','').replace('ml','')
            value = int(value)/1000
        return value
    
    def convert_products_weight(self, products_data):
        products_data.loc[:,'weight'] = products_data.loc[:,'weight'].astype('str').apply(lambda x:self.correct_format(x))
        products_data.loc[:,'weight'] = products_data.loc[:,'weight'].astype('str').apply(lambda x:self.convert_oz(x))
        products_data.loc[:,'weight'] = products_data.loc[:,'weight'].astype('str').apply(lambda x:self.convert_grams(x))
        # remove kg sign
        products_data.loc[:,'weight'] = products_data.loc[:,'weight'].astype('str').apply(lambda x:re.sub('[kg]','',x)) 
        # drop non numerical values with weight is digit
        products_data.loc[:,'weight'] = products_data[products_data.loc[:,'weight'].astype('str').apply(lambda x:x.replace('.','').isdigit())] 
        # convert all weight values to float and round up 2.d.p
        products_data.loc[:,'weight'] = products_data.loc[:,'weight'].astype('float').apply(lambda x: round(x,2))
        return products_data

    def clean_products_data(self, products_data):
        # Delete duplicate indexer column 
        products_data = products_data.iloc[:,1:]
        # Reset indexing
        index = [row for row in range(0,len(products_data))] 
        products_data['index'] = index
        products_data = products_data.set_index(['index'])
        # Drop NaN values
        products_data = products_data[products_data.weight != 'NULL']
        products_data = products_data.dropna()
        # Drop duplicates
        products_data = products_data.drop_duplicates()
        # Format dates
        products_data.loc[:,'date_added'] = (
        products_data['date_added'].apply(pd.to_datetime, errors='coerce'))
        return products_data


if __name__ == '__main__':
    pass