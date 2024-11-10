import json 
import logging 

import requests
import pandas as pd
from google.cloud import storage, bigquery


def get_data(url, bucket_name, output_path):


    publibike_data = requests.get(url=url)
    logging.info(f"The data received from the API is: {publibike_data.text}")
    logging.info(f"The status code received from the API is: {publibike_data.status_code}")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(output_path)

    with blob.open(file=output_path, mode="w") as file:
        file.write(publibike_data.text)

    logging.info(f"Data written at: {output_path}")

    return "200"

def transform_data(input_path, bucket_name, output_path, mode):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(input_path)

    with blob.open(file=input_path, mode="r") as file:
        raw_data = json.load(file)

    if mode == "stations":

        logging.info(f"The list of stations is: {raw_data.keys()}")

        list_of_stations = list()

        for station in range(len(raw_data.get("stations"))):

            raw_data_one_station = raw_data.get("stations")[station]
            data_one_station = dict()
            data_one_station["station_id"] = raw_data_one_station.get("id")
            data_one_station["latitude"] = raw_data_one_station.get("latitude")
            data_one_station["longitude"] = raw_data_one_station.get("longitude")
            data_one_station["state_id"] = raw_data_one_station.get("state").get("id")
            data_one_station["state_name"] = raw_data_one_station.get("state").get("name")
            data_one_station["name"] = raw_data_one_station.get("name")
            data_one_station["address"] = raw_data_one_station.get("address")
            data_one_station["zip"] = raw_data_one_station.get("zip")
            data_one_station["city"] = raw_data_one_station.get("city")
            data_one_station["network_id"] = raw_data_one_station.get("network").get("id")
            data_one_station["network_name"] = raw_data_one_station.get("network").get("name")
            data_one_station["is_virtual_station"] = raw_data_one_station.get("is_virtual_station")
            data_one_station["capacity"] = raw_data_one_station.get("capacity")

            list_of_stations.append(data_one_station)


        df_all_stations = pd.DataFrame.from_records(data=list_of_stations)

        logging.info(df_all_stations.head())
        logging.info(df_all_stations.info())

        df_all_stations.to_csv(path_or_buf=f"gs://{output_path}", index=False)
        logging.info(f"Transformed data written at gs://{output_path}")

    elif mode == "capacity":
        
        list_of_stations_with_capacity = list()

        for station in range(len(raw_data.get("stations"))):

            raw_data_one_station = raw_data.get("stations")[station]

            for bike in range(len(raw_data_one_station.get("vehicles"))):

                raw_data_one_bike = raw_data_one_station.get("vehicles")[bike]
                data_one_bike = dict()
                data_one_bike["station_id"] = raw_data_one_station.get("id")
                data_one_bike["vehicule_id"] = raw_data_one_bike.get("id")
                data_one_bike["vehicule_name"] = raw_data_one_bike.get("name")
                data_one_bike["vehicule_ebike_battery_level"] = raw_data_one_bike.get("ebike_battery_level")
                data_one_bike["vehicule_type_id"] = raw_data_one_bike.get("type").get("id")
                data_one_bike["vehicule_type_name"] = raw_data_one_bike.get("type").get("name")

                list_of_stations_with_capacity.append(data_one_bike)

        df_all_bikes = pd.DataFrame.from_records(data=list_of_stations_with_capacity)
        logging.info(df_all_bikes.head())
        logging.info(df_all_bikes.info())
        df_all_bikes.to_csv(path_or_buf=f"gs://{output_path}", index=False)
        logging.info(f"Transformed data written at gs://{output_path}")
    
    return "200"



def ingest_data(input_path, project_id, dataset, table, mode):

    df_transformed = pd.read_csv(filepath_or_buffer=f"gs://{input_path}")
    # Construct a BigQuery client object.
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.{table}"

    if mode == "stations":

        job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema=[
            # Specify the type of columns whose type cannot be auto-detected. For
            # example the "title" column uses pandas dtype "object", so its
            # data type is ambiguous.
            bigquery.SchemaField("station_id", bigquery.enums.SqlTypeNames.STRING),
            # Indexes are written if included in the schema by name.
            bigquery.SchemaField("latitiude", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("longitude", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("state_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("state_name", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("address", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("zip", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("city", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("network_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("network_name", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("is_virtual_station", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("capacity", bigquery.enums.SqlTypeNames.STRING),
        ],
        write_disposition="WRITE_APPEND",
        )

    elif mode == "capacity":
        job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema=[
            # Specify the type of columns whose type cannot be auto-detected. For
            # example the "title" column uses pandas dtype "object", so its
            # data type is ambiguous.
            bigquery.SchemaField("station_id", bigquery.enums.SqlTypeNames.STRING),
            # Indexes are written if included in the schema by name.
            bigquery.SchemaField("vehicle_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("vehicle_name", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("vehicle_ebike_battery_level", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("vehicle_type_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("vehicle_type_name", bigquery.enums.SqlTypeNames.STRING),
        ],
        write_disposition="WRITE_APPEND",
        )

    job = client.load_table_from_dataframe(
        df_transformed, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    logging.info(
        f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

    return "200"
