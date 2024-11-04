from sqlalchemy import create_engine
import pandas as pd

def ingest_mysql(host, user, password, db, query):
    try:
        engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{db}")

        df = pd.read_sql_query(query, engine)

        return df
    except Exception as e:
        print(f"Database connection failed: {e}")