import boto3
from mojap_metadata import Metadata
from mojap_metadata.converters.glue_converter import GlueConverter
from mojap_metadata.converters.etl_manager_converter import EtlManagerConverter

# DB version
db_version = 'db_v2'

# Evironment (will be passed as argument to database name and locations)
env = 'dev'

# Database name
db_name = f'matrix_db_{env}'

# Database location
db_location = f"s3://alpha-dag-matrix/db/{env}"

# Bookings table
meta_bookings = Metadata(name = "bookings",
                    description = "Information on bookings (e.g. desks, car park spaces, meeting rooms)",
                    columns = [
                         
                            {"name": "id", "type": "string", "description": "The internal ID of the location"},
                            {"name": "time_from", "type": "timestamp(ns)", "description": "The start date and time of the booking in the time zone of the resource"},

                            {"name": "time_to", "type": "timestamp(ns)", "description": "The end date and time of the booking in the time zone of the resource"},

                            {"name": "location_id", "type": "string", "description": "The internal ID of the booked resource"},
                            {"name": "location_kind", "type": "string", "description": "Indication the kind of location, typically one of BUILDING, FLOOR, ZONE, AREA, ROOM, DESK_BANK or DESK"},
                            {"name": "status", "type": "string", "description": "Current status of the booking. One of APPROVED, TENTATIVE or CANCELLED"},
                            {"name": "has_started", "type": "bool", "description": "Indicates whether the start time of the booking (time_from) is in the past. This does not indicate whether a booking has been 'started' (also known as checking in)."},
                            {"name": "has_ended", "type": "bool", "description": "Indicates whether the end time of the booking (time_to) is in the past."},
                            {"name": "check_in_status", "type": "string", "description": "Not included in API docs: Typically one of NOT_REQUIRED or CHECKED_IN"},
                            {"name": "attendee_count", "type": "int32", "description": "The number of attendees, including the owner if ownerIsAttendee is true."},
                            {"name": "owner_is_attendee", "type": "bool", "description": "Indicates whether the booking's owner is attending the meeting."},
                            {"name": "source", "type": "string", "description": "A value representing the Matrix Booking app used to create the booking."},
                            {"name": "version", "type": "string", "description": "Version of app used to make booking"},
                            {"name": "has_external_notes", "type": "bool", "description": "True if there are external notes on booking"},
                            {"name": "owner_id", "type": "string", "description": "The id of the (internal) person to whom the booking is assigned. This person is also known as the meeting organiser."},
                            {"name": "booked_by_id", "type": "string", "description": "The id of the user who created the booking."},
                            {"name": "organisation_id", "type": "string", "description": "The ID of the host organisation. This is only applicable to customers using cross-organisation resource sharing."},
                        {"name": "organisation_name", "type": "string", "description": "The name of the host organisation. This is only applicable to customers using cross-organisation resource sharing."},
                            {"name": "duration_milliseconds", "type": "int64", "description": "Duration of booking in milliseconds"},
                            {"name": "possible_actions_edit", "type": "bool", "description": "Possible actions for booking: edit"},
                            {"name": "possible_actions_cancel", "type": "bool", "description": "Possible actions for booking: cancel"},
                            {"name": "possible_actions_approve", "type": "bool", "description": "Possible actions for booking: approve"},
                            {"name": "possible_actions_confirm", "type": "bool", "description": "Possible actions for booking: confirm"},
                            {"name": "possible_actions_end_early", "type": "bool", "description": "Possible actions for booking: end early"},
                            {"name": "possible_actions_change_owner", "type": "bool", "description": "Possible actions for booking: chage owner"},
                            {"name": "possible_actions_start", "type": "bool", "description": "Possible actions for booking: start"},
                            {"name": "possible_actions_view_history", "type": "bool", "description": "Possible actions for booking: view history"},
                            {"name": "audit_created_created", "type": "timestamp(ns)", "description": "The specific history records for when the booking was created, approved, cancelled and checkedIn, as applicable to the booking. Cancelled bookings include those that required approval but were rejected."},
                            {"name": "audit_created_when", "type": "timestamp(ns)", "description": "The specific history records for when the booking was created, approved, cancelled and checkedIn, as applicable to the booking. Cancelled bookings include those that required approval but were rejected."},
                            {"name": "audit_created_event_type", "type": "string", "description": "The specific history records for when the booking was created, approved, cancelled and checkedIn, as applicable to the booking. Cancelled bookings include those that required approval but were rejected."},
                            {"name": "audit_created_event_user_id", "type": "string", "description": "The specific history records for when the booking was created, approved, cancelled and checkedIn, as applicable to the booking. Cancelled bookings include those that required approval but were rejected."},
                            {"name": "booking_group_id", "type": "string", "description": "ID for repeat bookings"},
                            {"name": "booking_group_type", "type": "string", "description": "Type of repeat bookings (e.g. REPEAT)"},
                            {"name": "booking_group_repeat_kind", "type": "string", "description": "How foten to repeat (e.g. DAILY)"},
                            {"name": "booking_group_repeat_start_date", "type": "timestamp(ns)", "description": "Date and time for start of the repeat of bookings"},
                            {"name": "booking_group_repeat_end_date", "type": "timestamp(ns)", "description": "Date and time for end of the repeat of bookings"},
                            {"name": "booking_group_repeat_text", "type": "string", "description": "A long text description of the type of repeat booking (e.g. 'Repeats  daily until Thu, 30 Nov 2023 (except Fri 13 Oct)')"},
                            {"name": "booking_group_status", "type": "string", "description": "Current status of the repeat booking. One of APPROVED, TENTATIVE or CANCELLED"},
                            {"name": "booking_group_first_booking_status", "type": "string", "description": "Current status of the first booking in the repeat booking"},
                            {"name": "status_reason", "type": "string", "description": "Current status text (e.g. CANCELLED_BY_OWNER)"},
                            {"name": "audit_cancelled_created", "type": "timestamp(ns)", "description": "For cancelled events: Date and time for cancelled booking - created"},
                            {"name": "audit_cancelled_when", "type": "timestamp(ns)", "description": "For cancelled events: Date and time for cancelled booking - when (? Not clear from API doc)"},
                            {"name": "audit_cancelled_event_type", "type": "string", "description": "For cancelled events: Description of cancelled event type"},
                            {"name": "audit_cancelled_event_user_id", "type": "string", "description": "For cancelled events: ID of user who cancelled booking"},
                            {"name": "source_version", "type": "string", "description": ""},
                            {"name": "is_booked_on_behalf", "type": "bool", "description": ""},
                            {"name": "audit_approved_created", "type": "timestamp(ns)", "description": "For approved events: Date and time created"},
                            {"name": "audit_approved_when", "type": "timestamp(ns)", "description": "For approved events: Date and time - when (? Not clear from API docs)"},
                            {"name": "audit_approved_event_type", "type": "string", "description": "For approved events: Description of approval event type"},
                            {"name": "audit_approved_event_user_id", "type": "string", "description": "For approved events: ID of user who approved event"},
                            {"name": "audit_checked_in_created", "type": "timestamp(ns)", "description": "For checked in events: Date and time created"},
                            {"name": "audit_checked_in_when", "type": "timestamp(ns)", "description": "For checked in events: Date and time - when (? Not clear from API docs)"},
                            {"name": "audit_checked_in_event_type", "type": "string", "description": "For checked in events: Description of checked in event type"},
                            {"name": "audit_checked_in_event_user_id", "type": "string", "description": "For checked in events: ID of user who approved event"},
                            {"name": "setup_time_from", "type": "timestamp(ns)", "description": "Not clear from API docs"},
                            {"name": "setup_time_to", "type": "timestamp(ns)", "description": "Not clear from API docs"},
                            {"name": "teardown_time_from", "type": "timestamp(ns)", "description": "Not clear from API docs"},
                            {"name": "teardown_time_to", "type": "timestamp(ns)", "description": "Not clear from API docs"},
   
                
                    ],
                    
                    file_format = 'parquet'
                   )

# Locations table
meta_locations = Metadata(name = "locations",
                    description = "Information on locations. Each resource (desk, room, etc.) is technically a location",
                    columns = [{"name": "id", "type": "string", "description": "The internal ID of the location"},
                               {"name": "organisation_id", "type": "string", "description": "The ID of the host organisation. This is only applicable to customers using cross-organisation resource sharing."},
                                                       {"name": "organisation_name", "type": "string", "description": "The name of the host organisation. This is only applicable to customers using cross-organisation resource sharing."},

                               {"name": "parent_id", "type": "string", "description": "The internal ID of the parent location (hierarchy of locations)"},
                               {"name": "kind", "type": "string", "description": "The type of location (e.g. building, floor, zone, desk)"},
                               {"name": "capacity", "type": "string", "description": "Capacity of location"},
                               {"name": "minimum_capacity", "type": "string", "description": "Not included in API docs: Minimum capacity of location"},
                               {"name": "name", "type": "string", "description": "The name of the location or resource"},
                               {"name": "short_qualifier", "type": "string", "description": "Short text that can be used to distinguish between two locations or resources with the same name. For example, two buildings might both have a meeting room called Room 101. The short qualifier is usually the name of the building."},
                               {"name": "long_qualifier", "type": "string", "description": "A longer text that can be used to distinguish between two locations/resources with the same name. For example, two floors might both have a desk bank called Desk Bank A. The long qualifier might be the name of the floor and building separated by a comma."},
                               {"name": "qualified_name", "type": "string", "description": "Combination of information from name, short_qualifier and long_qualifier"},
                               {"name": "long_name", "type": "string", "description": "Long text - more information on name of resource"},
                               {"name": "short_name", "type": "string", "description": "Short text - more information on name of resource"},
                               {"name": "description", "type": "string", "description": "Not included in API docs: Description of resource on system"},
                               {"name": "alert", "type": "string", "description": "Text information alerted to user"},
                               {"name": "is_bookable", "type": "bool", "description": "True for a location that is a bookable resource. Such locations will also have a booking category id."},
                               
                               {"name": "is_flex", "type": "bool", "description": "Not included in API docs: True if space is a flexible space"},

                               {"name": "external_reference", "type": "float64", "description": "Not included in API docs"},
                               {"name": "booking_category_id", "type": "string", "description": "The id of the corresponding booking category for a bookable resource, such as a desk or meeting room"},
                               {"name": "availability_type", "type": "string", "description": "Not included in API docs"},
                               {"name": "settings_time_zone_id", "type": "string", "description": "Not included in API docs"},
                               
                               {"name": "provider_id", "type": "string", "description": "Not included in API docs"},
                               {"name": "provider_notificaftion_days_interval", "type": "float64", "description": "Not included in API docs"},

                               {"name": "left", "type": "int64", "description": "The location or resource's left value as described in the Location hierarchy section (see API documentation)"},
                               {"name": "right", "type": "int64", "description": "The location or resource's right value as described in the Location hierarchy section (see API documentation)"},

                              
                               
                        ],
                    file_format = 'parquet'
                   )


meta_joined_rooms = Metadata(name="joined_rooms", 
                             description = "Manually uploaded data on joined rooms",
                             columns =[
                                 {"name": "joined_id", "type": "string", "description": "ID for"},
                                 {"name": "joined_name", "type": "string", "description": ""},
                                 {"name": "split_id", "type": "string", "description": ""},
                                 {"name": "split_name", "type": "string", "description": ""},
                                 {"name": "building", "type": "string", "description": ""},
                                 ],
                             file_format = 'csv'
                            )
if __name__ == "__main__":
    
    # Write schemas to 'old' etl_manager format (dependency on ....)
    
    # Initiate convertor
    etlc = EtlManagerConverter()
    
    # Convert and write out schemas
    etlc.generate_from_meta(meta_bookings).write_to_json(f"metadata/{db_version}/{env}/bookings.json")
    etlc.generate_from_meta(meta_locations).write_to_json(f"metadata/{db_version}/{env}/locations.json")
    etlc.generate_from_meta(meta_joined_rooms).write_to_json(f"metadata/{db_version}/{env}/joined_rooms.json")
    
    # Write schemas to metadata folder
#    meta_bookings.to_json(f"metadata/{db_version}/{env}/bookings.json")
#    meta_locations.to_json(f"metadata/{db_version}/{env}/locations.json")
#    meta_joined_rooms.to_json(f"metadata/{db_version}/{env}/joined_rooms.json")

    # Convert tables to glue schema

    # Initialise glue converter
    gc = GlueConverter()

    # Bookings schema
    schema_bookings = gc.generate_from_meta(meta_bookings, database_name=db_name, table_location=f"{db_location}/bookings")

    # Locations schema
    schema_locations = gc.generate_from_meta(meta_locations, database_name=db_name, table_location=f"{db_location}/locations")
    
    # Joined rooms
    schema_joined_rooms = gc.generate_from_meta(meta_joined_rooms, database_name=db_name, table_location=f"{db_location}/joined_rooms")

    # Create database

    # Client
    glue_client = boto3.client('glue')

    # Drop database
    glue_client.delete_database(Name=db_name)

    # Create the database
    glue_client.create_database(DatabaseInput={
                                    'Name': db_name,
                                    'Description': f'Database for storing tables extracted from the matrix booking system API: {env} environment',
                                    'LocationUri': f'alpha-dag-matrix/db/{env}/'
                                        }

                               )

    # Add tables

    # Bookings table
    glue_client.create_table(**schema_bookings) 

    # Locations table
    glue_client.create_table(**schema_locations)
    
    # Joined rooms
    glue_client.create_table(**schema_joined_rooms)