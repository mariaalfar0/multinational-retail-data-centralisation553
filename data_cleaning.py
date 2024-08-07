import re
import numpy as np 
import pandas as pd
from datetime import datetime
import uuid 

class DataCleaning:
    # Methods to clean data from each of the data sources.
    def __init__(self) -> None:
        pass
    
    # Check if UUID is valid
    def is_valid_uuid(self, val):
        try:
            uuid.UUID(str(val))
            return True
        except ValueError:
            return False
        
    # Remove alphabet characters from numeric columns <--- NOT WORKING
    def remove_letters(self, list):
        pattern = '[A-Z]'
        list = [re.sub(pattern, '', i) for i in list]
        return list

    def clean_user_data(self, user_data):
        # Get rid of duplicates
        user_data = user_data.drop_duplicates()
        # Drop null values 
        user_data.replace({'NULL': np.nan})    
        user_data = user_data.dropna()
        # Strip spaces etc. from start of strings
        name_cols = user_data.select_dtypes(object).columns
        user_data[name_cols] = user_data[name_cols].apply(lambda x: x.str.strip())
        # Check UUID validity + drop nonsense rows
        for val in user_data['user_uuid']:
            user_data['uuid_valid'] = self.is_valid_uuid(val)
        user_data = user_data[user_data.uuid_valid == True]
        user_data = user_data.drop('uuid_valid', axis = 1)
        # Fix typos in country codes and ensure that all data comes from GB, USA, DE
        user_data['country_code'] = user_data['country_code'].str.replace('GGB','GB')
        country_codes = ['GB', 'DE', 'US']
        user_data = user_data[user_data.country_code.isin(country_codes)]
        countries = ['United Kingdom', 'Germany', 'United States']
        user_data = user_data[user_data.country.isin(countries)]
        # Format dates correctly 
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
        card_data.replace({'NULL': np.nan})    
        card_data = card_data.dropna(thresh=2)
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
        # Drop NULL values
        stores_data.replace({'NULL': np.nan})    
        stores_data = stores_data.dropna(thresh=2)
        # Drop duplicates
        stores_data = stores_data.drop_duplicates()
        # Check staff_numbers is only digits
        stores_data['staff_numbers'] = self.remove_letters(stores_data['staff_numbers'])
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
        # Remove kg sign
        products_data.loc[:,'weight'] = products_data.loc[:,'weight'].astype('str').apply(lambda x:re.sub('[kg]','',x)) 
        # Drop non-numerical values with weight is digit
        products_data.loc[:,'weight'] = products_data[products_data.loc[:,'weight'].astype('str').apply(lambda x:x.replace('.','').isdigit())] 
        # Convert all weight values to float and roundto 2.d.p
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
        products_data.replace({'NULL': np.nan})    
        products_data = products_data.dropna(thresh=2)
        # Drop duplicates
        products_data = products_data.drop_duplicates()
        # Format dates correctly
        products_data.loc[:,'date_added'] = (
        products_data['date_added'].apply(pd.to_datetime, errors='coerce'))
        # Check UUID validity
        for val in products_data['uuid']:
           products_data['uuid_valid'] = self.is_valid_uuid(val)
        products_data = products_data[products_data.uuid_valid == True]
        products_data = products_data.drop('uuid_valid', axis = 1)
        return products_data
    
    def clean_orders_data(self, orders_data):
        orders_data = orders_data.drop(columns = ['first_name', 'last_name', '1'])
        # Check UUID validity + remove nonsense rows
        for val in orders_data['date_uuid']:
            orders_data['date_uuid_valid'] = self.is_valid_uuid(val)
        for val in orders_data['user_uuid']:
            orders_data['user_uuid_valid'] = self.is_valid_uuid(val)
        orders_data = orders_data[orders_data.date_uuid_valid == True]
        orders_data = orders_data[orders_data.user_uuid_valid == True]
        orders_data = orders_data.drop(columns = ['date_uuid_valid', 'user_uuid_valid', 'level_0'])
        return orders_data
    
    def clean_date_data(self, date_data):         
        # Drop null values
        date_data.replace({'NULL': np.nan})    
        date_data = date_data.dropna(thresh=2)
        # Check UUID validity + drop nonsense rows
        for val in date_data['date_uuid']:
            date_data['uuid_valid'] = self.is_valid_uuid(val)
        date_data = date_data[date_data.uuid_valid == True]
        date_data = date_data.drop('uuid_valid', axis = 1)
        # Format time correctly
        date_data['date_string'] = date_data['day'] + '/' + date_data['month'] + '/' + date_data['year'] + ' ' + date_data['timestamp']
        date_data.loc[:,'date_string'] = \
        date_data.loc[:,'date_string'].apply(pd.to_datetime,dayfirst=True,errors='coerce')
        return date_data

if __name__ == '__main__':
    pass