from utils import get_data, transform_data, ingest_data
URL = "https://api.publibike.ch/v1/public/partner/stations"
BUCKET_NAME = "ind-publibike-dev"
PROJECT_ID = "gold-circlet-433614-k2"

def extract_transform_load(request):

    get_data(
        url=URL, 
        bucket_name=BUCKET_NAME,
        output_path="raw_data.json"
    )

    transform_data(
        input_path="raw_data.json",
        bucket_name=BUCKET_NAME,
        output_path="transformed_data_stations.csv",
        mode="stations"
    )

    transform_data(
        input_path="raw_data.json",
        bucket_name=BUCKET_NAME,
        output_path="transformed_data_bikes.csv",
        mode="capacity"
    )

    ingest_data(
        input_path="transformed_data_stations.csv", 
        project_id=PROJECT_ID,
        dataset="publibikes-dev", 
        table="stations", 
        mode="stations"
    )

    return "200"