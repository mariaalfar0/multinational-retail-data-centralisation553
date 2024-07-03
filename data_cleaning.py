import numpy as np 
from datetime import datetime

class DataCleaning:
    # Methods to clean data from each of the data sources.
    def __init__(self) -> None:
        pass

    def clean_user_data(self, user_db):
        # You will need clean the user data, look out for NULL values, 
        # errors with dates, incorrectly typed values and rows filled 
        # with the wrong information.

        user_data = user_db
        user_data = user_data.replace('NULL', np.nan)
        user_data = user_data.dropna()
        name_cols = user_data.select_dtypes(object).columns
        user_data[name_cols] = user_data[name_cols].apply(lambda x: x.str.strip())
        user_data['country_code'] = user_data['country_code'].str.replace('GGB','GB')
        country_codes = ['GB', 'DE', 'US']
        user_data = user_data[user_data.country_code.isin(country_codes)]
        countries = ['United Kingdom', 'Germany', 'United States']
        user_data = user_data[user_data.country.isin(countries)]

        #for v in data.columns:
        #     print(data[v].value_counts())
        
        ## Check for digits in first/last names
        #for v in data['last_name']:
        #    if any(char.isdigit() for char in v):
        #        print(v)

        #for v in data['phone_number']:
        #    if any(char.isalpha() for char in v):
        #        print(v)

        #print(data.head()) 
        #print(list(data.columns.values))
        #print(data.info())
        
        return user_data
    
    def clean_card_data(self, card_db):
        card_data = card_db
        card_data = card_data.replace('NULL', np.nan)
        card_data = card_data.dropna()
        return card_data


if __name__ == '__main__':
    pass