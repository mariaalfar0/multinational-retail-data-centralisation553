from database_utils import DatabaseConnector

class DataExtractor:
    # This class will work as a utility class, in it you will be creating 
    # methods that help extract data from different data sources.
    # The methods contained will be fit to extract data from a particular 
    # data source, these sources will include CSV files, an API and an S3 bucket.

    def __init__(self) -> None:
        pass
    
    def read_rds_table(self):
        import pandas as pd
        from sqlalchemy import text
        db_init = DatabaseConnector()
        db_engine = db_init.init_db_engine()
        with db_engine.connect() as connection:
            result = connection.execute(text("SELECT * FROM legacy_users"))
            self.result = pd.DataFrame(result)
            return self.result

x = DataExtractor()
print(x.read_rds_table())