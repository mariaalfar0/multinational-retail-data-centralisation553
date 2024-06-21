class DataCleaning:
    # Methods to clean data from each of the data sources.
    def __init__(self) -> None:
        pass

    def clean_user_data(self):
        # You will need clean the user data, look out for NULL values, 
        # errors with dates, incorrectly typed values and rows filled 
        # with the wrong information.
        from data_extraction import DataExtractor
        import numpy as np 

        extractor = DataExtractor()
        data = extractor.read_rds_table() 
        data = data.replace('NULL', np.nan)
        data = data.dropna()
        name_cols = data.select_dtypes(object).columns
        data[name_cols] = data[name_cols].apply(lambda x: x.str.strip())
        data['country_code'] = data['country_code'].str.replace('GGB','GB')
        country_codes = ['GB', 'DE', 'US']
        data = data[data.country_code.isin(country_codes)]
        countries = ['United Kingdom', 'Germany', 'United States']
        data = data[data.country.isin(countries)]

        for v in data.columns:
             print(data[v].value_counts())
        #print(data.head()) 
        #print(list(data.columns.values))
        #print(data.info())

x = DataCleaning()
print(x.clean_user_data())    