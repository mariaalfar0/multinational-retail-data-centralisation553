import yaml

class DatabaseConnector:
    # Use to connect with and upload data to 
    # the database.
    def __init__(self):
        pass

    def read_db_creds(self):
        with open('db_creds.yaml') as f:
            self.db_creds = yaml.load(f, Loader=yaml.SafeLoader)
            return self.db_creds
    
    def init_db_engine(self):
        from sqlalchemy import create_engine
        self.db_creds = self.read_db_creds()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = self.db_creds['RDS_HOST']
        USER = self.db_creds['RDS_USER']
        PASSWORD = self.db_creds['RDS_PASSWORD']
        DATABASE = self.db_creds['RDS_DATABASE']
        PORT = self.db_creds['RDS_PORT']
        db_engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return db_engine
    
    def list_db_tables(self):
        from sqlalchemy import inspect
        db_engine = self.init_db_engine()
        inspector = inspect(db_engine)
        return inspector.get_table_names()

    def upload_to_db(self):
        import pandas as pd
        #db_table.to_sql('dim_users', db_engine, if_exists='replace')
        pass

x = DatabaseConnector()
#x.list_db_tables()
x.upload_to_db()