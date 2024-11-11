from utils import get_data, transform_data, ingest_data
URL = "https://api.publibike.ch/v1/public/partner/stations"
BUCKET_NAME = "ind-etl-publibike-dev"
PROJECT_ID = "gold-circlet-433614-k2"

from datetime import datetime



def extract_transform_load(request):

    now = datetime.now()
    current_full_date = now.strftime("%Y-%m-%d %H:%M:00")
    current_ymd = now.strftime("%Y-%m-%d")
    current_hour = now.strftime("%H:00:00")
    current_minute = now.strftime("%H:%M:00")

    get_data(
        url=URL, 
        bucket_name=BUCKET_NAME,
        output_path=f"raw_data/{current_ymd}/{current_hour}/data.json"
    )

    transform_data(
        input_path=f"raw_data/{current_ymd}/{current_hour}/data.json",
        bucket_name=BUCKET_NAME,
        output_path=f"transformed_data/{current_ymd}/{current_hour}/{current_minute}_transformed_data_stations.csv",
        mode="stations",
        date_time=current_full_date
    )

    transform_data(
        input_path=f"raw_data/{current_ymd}/{current_hour}/data.json",
        bucket_name=BUCKET_NAME,
        output_path=f"transformed_data/{current_ymd}/{current_hour}/{current_minute}_transformed_data_bikes.csv",
        mode="capacity",
        date_time=current_full_date
    )

    ingest_data(
        input_path=f"transformed_data/{current_ymd}/{current_hour}/{current_minute}_transformed_data_stations.csv", 
        bucket_name=BUCKET_NAME,
        project_id=PROJECT_ID,
        dataset="publibike_dev", 
        table="stations", 
        mode="stations"
    )

    ingest_data(
        input_path=f"transformed_data/{current_ymd}/{current_hour}/{current_minute}_transformed_data_bikes.csv", 
        bucket_name=BUCKET_NAME,
        project_id=PROJECT_ID,
        dataset="publibike_dev", 
        table="capacity", 
        mode="capacity"
    )

    return "200"