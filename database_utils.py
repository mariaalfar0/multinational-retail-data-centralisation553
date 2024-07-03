import yaml

class DatabaseConnector:
    # Use to connect with and upload data to 
    # the database.
    def __init__(self):
        pass

    def read_db_creds(self, yaml_file):
        with open(yaml_file) as f:
            db_creds = yaml.load(f, Loader=yaml.SafeLoader)
            #print(db_creds)
            return db_creds
    
    def init_db_engine(self, creds):
        from sqlalchemy import create_engine
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = creds['RDS_HOST']
        USER = creds['RDS_USER']
        PASSWORD = creds['RDS_PASSWORD']
        DATABASE = creds['RDS_DATABASE']
        PORT = creds['RDS_PORT']
        db_engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return db_engine
    
    def list_db_tables(self, engine):
        from sqlalchemy import inspect
        inspector = inspect(engine)
        print(inspector.get_table_names())
        return inspector.get_table_names()

    def upload_to_db(self, df, table_name, engine):
        # engine = self.init_db_engine(yaml_file)
        df.to_sql(table_name, con = engine, if_exists='replace', index=False)

if __name__ == '__main__':
    x = DatabaseConnector()
    y = x.read_db_creds('local_db_creds.yaml')
    print(y)
    print(type(y))
    local_db_engine = x.init_db_engine(y)
    print(local_db_engine)
    pass