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


--level-1
SELECT
* 
FROM 
    @dev_db.stage_sch.raw_stg
    (FILE_FORMAT => JSON_FILE_FORMAT) t;


--level-2
SELECT
    TRY_TO_TIMESTAMP(t.$1:records[0].last_update::text , 'dd-mm-yyyy hh24:mi:ss'  ) AS index_record_ts,
    t.$1,
    t.$1:total::int AS record_count,
    t.$1:version::text AS json_version
FROM 
    @dev_db.stage_sch.raw_stg
    (FILE_FORMAT => JSON_FILE_FORMAT) t;


--level-3     
    
    
