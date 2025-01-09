from google.cloud import bigquery
import streamlit as st

PROJECT_ID = "inspired-victor-442419-j3"
bigquery_client = bigquery.Client(project=PROJECT_ID)

sql_query = f"""
    SELECT
        * 
    FROM `inspired-victor-442419-j3.publibike.stations_capacity_aggregated`
    ORDER BY station_id, ingestion_time
"""

df_stations_capacity_aggregated = bigquery_client.query(sql_query).to_dataframe()

df_filtered = df_stations_capacity_aggregated.loc[df_stations_capacity_aggregated["station_id"] == "10", ]

st.dataframe(data=df_filtered)

st.line_chart(data=df_filtered, x="ingestion_time", y="nb_bikes_available")