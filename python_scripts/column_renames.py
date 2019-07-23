import json
bookings_renames = {'id': 'id',
                    'timeFrom': 'time_from',
                    'timeTo': 'time_to',
                    'audit.created.created': 'created',
                    'locationId': 'location_id',
                    'status': 'status',
                    'statusReason': 'status_reason'}

with open("metadata/bookings_renames.json", "w") as fp:
    json.dump(bookings_renames, fp)
    
location_renames = {'id': 'id',
                    'name': 'name',
                    'longQualifier': 'long_qualifier',
                    'capacity': 'capacity'}

with open("metadata/locations_renames.json", "w") as fp:
    json.dump(location_renames, fp)