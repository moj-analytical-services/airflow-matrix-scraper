from functions.general_helpers import get_command_line_arguments
from dateutil.parser import parse

# Get command line arguments
args = get_command_line_arguments()

""" 
Database constants 
"""

# Database name
db_name = f"matrix_{args.env}"

db_location = f"s3://alpha-dag-matrix/db/{args.env}"

region_name = "eu-west-1"

""" 
Database tables
"""

# Bookings

meta_path_bookings = f"metadata/{args.env}/bookings.json"
table_location_bookings = f"{db_location}/bookings"
column_renames = ""

# Locations
meta_path_locations = f"metadata/{args.env}/locations.json"
table_location_locations = f"{db_location}/locations"

# Joined rooms
meta_path_joined_rooms = f"metadata/{args.env}/joined_rooms.json"
table_location_joined_rooms = f"{db_location}/joined_rooms"

"""paths"""
suffix = "" if args.env == "prod" else f"-{args.env}"

# Land locations
land_bucket = f"mojap-land{suffix}"
land_location = f"s3://{land_bucket}/corporate/matrix"

# Raw history locations
raw_hist_bucket = f"mojap-raw-hist{suffix}"

"""parsed args"""

scrape_date = parse(args.scrape_date).strftime("%Y-%m-%d")
env = args.env
function_to_run = args.function
