from mysql_helper import ingest_mysql

query = """SELECT
                *
            FROM player_allstar
            WHERE season_id BETWEEN 1981 AND 1983 """

dataframe = ingest_mysql("db.relational-data.org",
                            "guest",
                            "relational",
                            "Basketball_men",
                            query
                            )

print(dataframe.head(50))
print(dataframe.info())