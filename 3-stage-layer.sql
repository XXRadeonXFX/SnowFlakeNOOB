USE ROLE sysadmin;
USE SCHEMA dev_db.stage_sch;
USE WAREHOUSE adhoc_wh;


-- CREATE INTERNAL STAGE AND ENABLE DIRECTORY SERVICE
CREATE STAGE IF NOT EXISTS  raw_stg
directory = ( enable = true )
comment = 'all the air quality raw data will store in this internal stage location'
;


-- CREATE FILE FORMAT TO PROCESS THE JSON FILE
CREATE FILE FORMAT IF NOT EXISTS json_file_format
    type = 'JSON'
    compression = 'AUTO'
    comment = 'this is json file format object'
;


SHOW STAGES;
LIST @raw_stg;
