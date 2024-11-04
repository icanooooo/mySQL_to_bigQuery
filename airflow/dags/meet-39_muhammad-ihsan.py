from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from helper.bigquery_helper import create_client, load_bigquery, create_table, create_schema
from helper.mysql_helper import ingest_mysql

def ingest_data_mysql():
    query = """SELECT
                    *
                FROM player_allstar
                WHERE season_id = 1985"""

    dataframe = ingest_mysql("db.relational-data.org",
                                "guest",
                                "relational",
                                "Basketball_men",
                                query
                                )

    dataframe['ingestion_time'] = datetime.now()

    return dataframe

def load_data_bigquery(**kwargs):
    client = create_client()
    dataframe = kwargs['ti'].xcom_pull(task_ids='extract_mysql')
    table_id = "purwadika.ihsan.meet-39_muhammad-ihsan"
    
    load_bigquery(client, dataframe, table_id, "WRITE_APPEND", 'ingestion_time')

    print(f"loaded {dataframe.shape[0]} rows to {table_id}")

with DAG('mySql_to_bigquery',
         start_date=datetime(2024, 11, 2),
         description='DAG to load nba table to Bigquery',
         tags=['meet-39'],
         schedule='@daily',
         catchup=False) as dag:
    
    ingest_data = PythonOperator(task_id='extract_mysql', python_callable=ingest_data_mysql)
    load_data = PythonOperator(task_id='load_to_bigquery', python_callable=load_data_bigquery)

    ingest_data >> load_data    