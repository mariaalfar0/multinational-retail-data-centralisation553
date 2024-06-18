import yaml

class DatabaseConnector:
    # Use to connect with and upload data to 
    # the database.
    def __init__(self):
        pass

    def read_db_creds(self):
        with open('db_creds.yaml') as f:
            self.db_creds = yaml.load(f, Loader=yaml.SafeLoader)
    
    def init_db_engine(self):
        from sqlalchemy import create_engine
        import pandas as pd
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = self.db_creds['RDS_HOST']
        USER = self.db_creds['RDS_USER']
        PASSWORD = self.db_creds['RDS_PASSWORD']
        DATABASE = self.db_creds['RDS_DATABASE']
        PORT = self.db_creds['RDS_PORT']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine
        

                
        
x = DatabaseConnector()
x.read_db_creds()
x.init_db_engine()