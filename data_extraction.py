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
            self.result = pd.DataFrame(result)
            return self.result


if __name__ == '__main__':
    pass