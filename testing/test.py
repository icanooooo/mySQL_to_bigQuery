from bigquery_helper import create_client, create_table_with_time_partition, create_schema, load_bigquery
from mysql_helper import ingest_mysql
from datetime import datetime

query = """SELECT
            *
            FROM player_allstar
            WHERE season_id = 1985"""

dataframe = ingest_mysql("db.relational-data.org",
                         "guest",
                         "relational",
                         "Basketball_men",
                         query)


dataframe['ingestion_time'] = datetime.now()

print(dataframe)
print(dataframe.info())

table_id = 'purwadika.ihsan.meet-39_test'

client = create_client('../keys/bigquery-project.json')

create_table_with_time_partition(client, table_id, create_schema(dataframe), 'ingestion_time')
load_bigquery(client, dataframe, table_id, 'ingestion_time')