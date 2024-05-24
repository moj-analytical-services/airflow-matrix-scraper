# airflow-matrix-scraper

This is a scraper for the matrixbooking api, to scrape bookings data and associated metadata into an Athena database, to allow for easier analysis with automatically updating data. The approach is modelled on [the scraper for the Occupeye api](https://github.com/moj-analytical-services/airflow-occupeye-scraper). While it's geared towards the automated scrape use case, the functions are intended to be transparent enough to allow for interactive analysis and modification. In addition, there is a notebook with some useful functions to act as a scratchpad.

## python_scripts/Database_builder.py

This script is used to define and create the Athena database, `matrix_db`. It creates the tables; defines the columns in the tables; exports the schema into JSON (more on that later) and finally creates the database. It remains in this repo both to demostrate how it was defined, as well as to allow for an easy interface if the schema needs to be changed - just edit the script and run again to rebuild (though you'll likely need to rescrape the data if you do so).

The database contains three tables: bookings, locations and joined_rooms. Bookings and locations are updated via the scraper. 

Joined_rooms is (currently) a manually-uploaded CSV, which maps the location_id that represents a booking across multiple rooms to the locations of the constituent rooms. For example, in 102PF, conference rooms 1A, 1B and 1C can be combined.

## python_scripts/main.py

This is the main function for use in Airflow. This defines a command line function to scrape a specified day's worth of bookings. In the context of the Airflow task, it's designed to scrape bookings that occurred (or should have occurred) yesterday. These daily snapshots are combined into an Athena database, which is then queried and combined with the Occupeye db via a CTAS query to create an app db that the matrixbooking app (https://github.com/moj-analytical-services/matrixbooking) will query in turn.

## python_scripts/api_requests.py

This contains the main functions for scraping data from the API. The API documentation is here: https://developers.matrixbooking. Note that at the time of writing, the method of authentication is different from what is described in the documentation. The api url is https://app.matrixbooking.com/api/v1 rather than https://api.matrixbooking.com, and authentication is controlled by POSTing to api/v1/users/login, and receiving a cookie in return. That occurs in the `matrix_authenticate(session)` function.

The main function, `scrape_days_from_api`, works as follows:
1. Constructs a query from some specified characters. At the time of writing, the function only allows the start and end dates to be set, while the other parameters are essentially hard-coded (but the functions are transparent if that needs to be changed).
2. Scrapes the first page of data via that query, and stores the resulting JSON in a list
3. If there are more bookings to extract (i.e. if the number of bookings returned = the specified pageSize), it scrapes the next page and adds the results to the list, and repeats until there are no more bookings to return.
4. It converts the results into a flattened dataframe via `get_bookings_df`, which:
    1. flattens the nested JSON structure through the pandas function `pd.io.json.json_normalize`
    2. Loads `metadata/bookings_renames.json`, which is a dictionary of key/value pairs corresponding respectively to the source column names and the desired Athena database column names
    3. Selects only the columns corresponding to the keys in `bookings_renames.json`, and renames them to the values.
    4. Imposes conformance to the Athena metadata
5. Saves to the S3 bucket named `{start_date}.parquet`
6. Does the same thing for locations

#### API Issues when filtering bookings by status

Confirmed / cancelled / tentative - if you specify these statuses in the booking API call, then it only returns a subset of actual bookings - if you don't it only returns non-cancelled ones. We don't include status because it doesn't return everything. 

#### Table API URL's

Bookings table - https://app.matrixbooking.com/api/v1/booking

Locations table - https://app.matrixbooking.com/api/v1/org/43/locations

Note: The location API data is only a snapshot of current locations, there is no historic location data available from the API. This means that the historical location data should not be overwritten. (See [Issue #56](https://github.com/orgs/moj-analytical-services/projects/102/views/11?pane=issue&itemId=63018774) for more details)

## python_scripts/column_renames.py

This script constructs the column rename files, so if they need to change (if the API and/or desired Athena schema changes), edit this script and rerun.

## python_scripts/refresh_app_db.py

This script contains a single function, composed of several [CTAS](https://docs.aws.amazon.com/athena/latest/ug/ctas.html) queries, which will delete and rebuild a synthesised database that matrixbooking can query. This database, `matrixbooking_app_db` contains the following tables

### bookings

This involves a slightly complex transformation, due to the need to deal with joined rooms. For bookings on multiple rooms, we need the location_id of the individual room in order to link to Occupeye, but still need to know that they're from joined rooms. It therefore starts with a subquery that left-joins the joined_rooms table, adds columns for the joined name and split name, and id, and uses coalesce() to use the split_id from joined_rooms if available, or just the location_id otherwise. The query then selects everything from this table, inner joins on the locations table to add the capacity and longQualifier (the zone and building), then inner joins on the Occupeye sensors metadata.

### locations

More simple. This just inner joins `matrix_db.locations` to `occupeye_db_live.sensors`, to get metadata on rooms from both matrix and occupeye.

### sensor_observations

Gets the sensor observations from Occupeye, attaches the metadata from the sensors table, and inner joins on `matrix_db.locations` so it only get rooms on matrix.

### surveys

This gets the survey name, start date and end date from the Occupeye surveys joined to the `matrixbooking_app_db.locations`, to get just the occupeye surveys that have matrix data (and hence can be selected in the app).
