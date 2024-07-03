import pandas as pd

class DataExtractor:
    # This class will work as a utility class, in it you will be creating 
    # methods that help extract data from different data sources.
    # The methods contained will be fit to extract data from a particular 
    # data source, these sources will include CSV files, an API and an S3 bucket.

    def __init__(self) -> None:
        pass
    
    def read_rds_table(self, engine, table_name):
        from sqlalchemy import text
        db_engine = engine
        with db_engine.connect() as connection:
            result = connection.execute(text(f"SELECT * FROM {table_name}"))
            result = pd.DataFrame(result)
            #print(result.columns.values)
            return result
        
    def retrieve_pdf_data(self, url):
        from tabula.io import read_pdf
        pdf_data = pd.concat(read_pdf(url, pages='all'))
        return pdf_data
    
    def list_number_of_stores(url, headers):
        import requests
        number_of_stores = requests.get(url, headers=headers)
        return number_of_stores


if __name__ == '__main__':
    pass