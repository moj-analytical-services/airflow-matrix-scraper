from etl_manager.meta import DatabaseMeta, TableMeta


db = DatabaseMeta(name="matrix_db", bucket="alpha-dag-matrix")

# Create table meta object
bookings = TableMeta(name="bookings", location="bookings", data_format="parquet")

# Add column defintions to the table
bookings.add_column(name="id", type="character", description="Booking id")
bookings.add_column(
    name="time_from", type="datetime", description="Start time of booking"
)
bookings.add_column(name="time_to", type="datetime", description="End time of booking")
bookings.add_column(
    name="created", type="datetime", description="Time the booking was created"
)
bookings.add_column(
    name="cancelled_time", type="datetime", description="Time of cancellation"
)
bookings.add_column(
    name="location_id", type="character", description="id to match to location"
)
bookings.add_column(
    name="owner_id", type="character", description="id of user who owns the booking"
)
bookings.add_column(
    name="booked_by_id",
    type="character",
    description="id of user who created the booking",
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
bookings.add_column(
    name="attendee_count", type="int", description="Self-reported number of attendees"
)

db.add_table(bookings)

locations = TableMeta(name="locations", location="locations", data_format="parquet")

locations.add_column(name="id", type="character", description="locationId")
locations.add_column(name="name", type="character", description="location name")
locations.add_column(
    name="long_qualifier", type="character", description="long qualifier for location"
)
locations.add_column(name="capacity", type="character", description="room capacity")
db.add_table(locations)

joined_rooms = TableMeta(
    name="joined_rooms", location="joined_rooms", data_format="csv"
)
joined_rooms.add_column(name="joined_id", type="character", description="joined id")
joined_rooms.add_column(name="joined_name", type="character", description="joined name")
joined_rooms.add_column(name="split_id", type="character", description="split id")
joined_rooms.add_column(name="split_name", type="character", description="split name")
joined_rooms.add_column(name="building", type="character", description="building")

db.add_table(joined_rooms)

bookings.write_to_json("metadata/bookings.json")
locations.write_to_json("metadata/locations.json")
joined_rooms.write_to_json("metadata/locations.json")

db.create_glue_database(delete_if_exists=True)