import pydbtools as pydb
from s3_utils import delete_all_matching_s3_objects


def refresh_app_db():
    pydb.get_athena_query_response(
        "drop database if exists matrixbooking_app_db cascade"
    )
    delete_all_matching_s3_objects("alpha-app-matrixbooking", "db")

    pydb.get_athena_query_response(
        """create database matrixbooking_app_db
        location 's3://alpha-app-matrixbooking/db/'"""
    )

    pydb.get_athena_query_response(
        """
        create table if not exists matrixbooking_app_db.bookings
        with(external_location = 's3://alpha-app-matrixbooking/db/bookings/')
        as select b.*, l.name, l.long_qualifier, l.capacity
        from matrix_db.bookings as b
        inner join matrix_db.locations as l
        on b.location_id = l.id
        inner join occupeye_db_live.sensors as s
        on l.id = s.location
        """
    )

    pydb.get_athena_query_response(
        """
        create table if not exists matrixbooking_app_db.locations
        with(external_location = 's3://alpha-app-matrixbooking/db/locations/')
        as select * from matrix_db.locations as l
                                   inner join occupeye_db_live.sensors as s
                                   on l.id = s.location
        """
    )

    pydb.get_athena_query_response(
        """
        create table if not exists matrixbooking_app_db.sensor_observations
        with(external_location =
        's3://alpha-app-matrixbooking/db/sensor_observations')
        as
        select so.obs_datetime, so.sensor_value, se.*
        from occupeye_db_live.sensor_observations as so
        inner join occupeye_db_live.sensors as se
        on so.survey_device_id = se.surveydeviceid
        inner join matrix_db.locations as l
        on l.id = se.location
        """
    )
    pydb.get_athena_query_response(
        """
        create table if not exists matrixbooking_app_db.surveys
        with(external_location = 's3://alpha-app-matrixbooking/db/surveys')
        as
        select distinct su.survey_id, su.name
        from occupeye_db_live.surveys as su
        inner join matrixbooking_app_db.locations as l
        on l.survey_id = su.survey_id
        """
    )
