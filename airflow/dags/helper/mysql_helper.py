from sqlalchemy import create_engine
import pandas as pd

def ingest_mysql(host, user, password, db, query):
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{db}")

    df = pd.read_sql_query(query, engine)

    return df