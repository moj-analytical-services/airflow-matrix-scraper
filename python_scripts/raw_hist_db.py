import awswrangler as wr
import boto3
from mojap_metadata import Metadata
from mojap_metadata.converters.glue_converter import GlueConverter

gc = GlueConverter()
glue_client = boto3.client("glue")
s3_client = boto3.client("s3")

env = "preprod"
db_name = f"matrix_{env}_raw_hist"
raw_hist = f"s3://mojap-raw-hist-{env}/corporate/matrix/pass/"

booking_data_path = f"{raw_hist}bookings/"

location_data_folder = f"{raw_hist}locations/"
##TODO Get list of location files of find the most recent
# s3://mojap-raw-hist/corporate/matrix/pass/locations/raw-2024-03-06-1-1709769683.jsonl
latest_file = "raw-2024-03-06-1-1709769683.jsonl" 
location_data_path = f"location_data_folder{latest_file}" 


booking_meta_data = Metadata.from_json(f"./metadata/db_v2/{env}/bookings_raw_hist.json")
# /Users/william.orr/Developer/Projects/airflow-matrix-scraper/metadata/db_v2/preprod/locations_raw_hist.json
location_meta_data = Metadata.from_json(f"./metadata/db_v2/{env}/locations_raw_hist.json")


booking_schema = gc.generate_from_meta(
    booking_meta_data, database_name=db_name, 
    table_location=booking_data_path)

# Note - originally tried to point athena at a single location file `location_data_path`
location_schema = gc.generate_from_meta(
    location_meta_data, database_name=db_name, 
    table_location=location_data_folder)

wr.catalog.create_database(name=db_name, exist_ok=True)
glue_client.create_table(**booking_schema)
glue_client.create_table(**location_schema)

'''
botocore.errorfactory.AlreadyExistsException: 
An error occurred (AlreadyExistsException) 
when calling the CreateTable operation: 
Table already exists
'''