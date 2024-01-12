import pydbtools as pydb
from s3_utils import delete_all_matching_s3_objects
from logging import getLogger

logger = getLogger(__name__)

db_app_name = "matrixbooking_app_db_v2"


def refresh_app_db(env: str = "dev"):
    logger.info(f"Refreshing {env} database")
    logger.info(f"Dropping current {env} database")
    pydb.get_athena_query_response(f"drop database if exists {db_app_name} cascade")

    logger.info(f"Delete {env} database files in S3 bucket")
    delete_all_matching_s3_objects("alpha-dag-matrix", f"db/{env}")

    logger.info(f"Create new {env} database")
    pydb.get_athena_query_response(
        f"""create database {db_app_name} location
        's3://alpha-dag-matrix/db/{env}'"""
    )

    logger.info("Create bookings table")
    pydb.get_athena_query_response(
        f"""
        create table if not exists {db_app_name}.bookings
        with(external_location = 's3://alpha-dag-matrix/db/{env}/bookings')
        as
        select distinct
        b.*,
        coalesce(b.joined_name, l.name) as name,
        l.long_qualifier,
        l.capacity
        from (select b.id, b.time_from, b.time_to, b.audit_created_created as created,
            b.audit_cancelled_created as cancelled_time,
            coalesce(jr.split_id, b.location_id) as location_id,
            b.owner_id,
            b.booked_by_id,
            b.status,
            b.status_reason,
            b.attendee_count,
            jr.split_id,
            jr.joined_name,
            jr.split_name
            from
            matrix_db_{env}.bookings as b
            left join matrix_db_{env}.joined_rooms as jr
            on b.location_id = jr.joined_id
            where b.location_kind = 'ROOM') as b
        inner join
        matrix_db_{env}.locations as l
        on b.location_id = l.id
        inner join occupeye_db_live.sensors as s
        on l.id = s.location
        """
    )

    logger.info("Create locations table")
    pydb.get_athena_query_response(
        f"""
        create table if not exists {db_app_name}.locations
        with(external_location = 's3://alpha-app-matrixbooking/db/{env}/locations/')
        as select * from matrix_db_{env}.locations as l
                                   inner join occupeye_db_live.sensors as s
                                   on l.id = s.location
        """
    )

    logger.info("Create sensor observations table")
    pydb.get_athena_query_response(
        f"""
        create table if not exists {db_app_name}.sensor_observations
        with(external_location =
        's3://alpha-app-matrixbooking/db/{env}/sensor_observations')
        as
        select so.obs_datetime, so.sensor_value, se.*
        from occupeye_db_live.sensor_observations as so
        inner join occupeye_db_live.sensors as se
        on so.survey_device_id = se.surveydeviceid
        inner join matrix_db_{env}.locations as l
        on l.id = se.location
        """
    )

    logger.info("Create surveys table")
    pydb.get_athena_query_response(
        f"""
        create table if not exists matrixbooking_app_db.surveys
        with(external_location = 's3://alpha-app-matrixbooking/db/{env}/surveys')
        as
        select distinct su.survey_id, su.name, su.startdate, su.enddate
        from occupeye_db_live.surveys as su
        inner join {db_app_name}.locations as l
        on l.survey_id = su.survey_id
        """
    )  # NOT BEEN QA'd yet
