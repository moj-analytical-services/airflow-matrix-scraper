from etl_manager.meta import DatabaseMeta, TableMeta


db = DatabaseMeta(name="matrix_db", bucket="alpha-dag-matrix")

# Create table meta object
bookings = TableMeta(name="bookings", location="bookings", data_format="parquet")

# Add column defintions to the table
bookings.add_column(name="id", type="int", description="Booking id")
bookings.add_column(
    name="time_from", type="datetime", description="Start time of booking"
)
bookings.add_column(name="time_to", type="datetime", description="End time of booking")
bookings.add_column(
    name="created", type="datetime", description="Time the booking was created"
)
bookings.add_column(
    name="location_id", type="int", description="id to match to location"
)
bookings.add_column(
    name="status",
    type="character",
    description="One of APPROVED, TENTATIVE or CANCELLED",
)
bookings.add_column(
    name="status_reason",
    type="character",
    description="Reason for cancellation where relevant",
)
db.add_table(bookings)

locations = TableMeta(
    name="locations", location="locations", data_format="parquet"
)

locations.add_column(name="id", type="int", description="locationId")
locations.add_column(name="name", type="character", description="location name")
locations.add_column(
    name="long_qualifier", type="character", description="long qualifier for location"
)
locations.add_column(name="capacity", type="double", description="room capacity")
db.add_table(locations)


bookings.write_to_json("metadata/bookings.json")
locations.write_to_json("metadata/locations.json")

db.create_glue_database(delete_if_exists=True)