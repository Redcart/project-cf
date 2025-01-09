from utils import run_query

URL = "https://api.publibike.ch/v1/public/partner/stations"
BUCKET_NAME = "proj-etl-publibike-dev"
PROJECT_ID = "inspired-victor-442419-j3"
DATASET = "publibike_dev"
TABLE = "stations_capacity_aggregated"

def process(request):

    run_query(
        project_id=PROJECT_ID,
        dataset=DATASET,
        table=TABLE,
        sql_file="query.sql"
    )

    return "200"