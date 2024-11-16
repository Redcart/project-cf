import logging 

from google.cloud import bigquery


def run_query(project_id, dataset, table, sql_file):

    with open(sql_file, "r") as file:
        sql_query = file.read()

    table_id = f"{project_id}.{dataset}.{table}"

    job_config = bigquery.QueryJobConfig(
        destination=table_id, 
        write_disposition='WRITE_APPEND'
    )
    
    bigquery_client = bigquery.Client(project=project_id)
    job = bigquery_client.query(sql_query, job_config=job_config)
    job.result()

    logging.info(f"Data written at: {project_id}.{dataset}.{table}")

    return "200"