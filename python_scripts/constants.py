from functions.general_helpers import get_command_line_arguments
from dateutil.parser import parse

# Get command line arguments
args = get_command_line_arguments()

""" 
Database constants 
"""

# Current db_version
db_version = "db_v2"

# Database name
db_name = f"matrix_db_{args.env}"

db_location = f"s3://alpha-dag-matrix/db/{args.env}"


""" 
Database tables
"""

# Bookings

meta_path_bookings = f"metadata/{db_version}/{args.env}/bookings.json"
table_location_bookings = f"{db_location}/bookings"
column_renames = ""

# Locations
meta_path_locations = f"metadata/{db_version}/{args.env}/locations.json"
table_location_locations = f"{db_location}/locations"

# Joined rooms
meta_path_joined_rooms = f"metadata/{db_version}/{args.env}/joined_rooms.json"
table_location_joined_rooms = f"{db_location}/joined_rooms"

# Raw history locations
suffix = "-dev" if args.env == "dev" else ""
land_location = f"s3://mojap-land{suffix}/corporate/matrix"


"""parsed args"""

scrape_date = parse(args.scrape_date).strftime("%Y-%m-%d")
env = args.env
function_to_run = args.function
