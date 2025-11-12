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
SELECT
    TRY_TO_TIMESTAMP(t.$1:records[0].last_update::text , 'dd-mm-yyyy hh24:mi:ss'  ) AS index_record_ts,
    t.$1,
    t.$1:total::int AS record_count,
    t.$1:version::text AS json_version,
    -- meta data information
    metadata$filename AS _stg_file_name,
    metadata$FILE_LAST_MODIFIED AS _stg_file_load_ts,
    metadata$FILE_CONTENT_KEY AS _stg_file_md5,
    CURRENT_TIMESTAMP() AS _copy_data_ts
    
FROM 
    @dev_db.stage_sch.raw_stg
    (FILE_FORMAT => JSON_FILE_FORMAT) t;    


CREATE OR REPLACE transient TABLE raw_aqi (
    id INT PRIMARY KEY AUTOINCREMENT,
    index_record_ts TIMESTAMP NOT NULL ,
    json_data VARIANT NOT NULL,
    record_count NUMBER NOT NULL DEFAULT 0,
    json_version TEXT NOT NULL,

    --audit Columns
    _stg_file_name TEXT,
    _stg_file_load_ts TIMESTAMP,
    _stg_file_md5 TEXT,
    _copy_data_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );

DESCRIBE TABLE raw_aqi;

CREATE OR REPLACE TASK copy_air_quality_data
    WAREHOUSE = load_wh
    SCHEDULE = 'USING CRON 0 * * * * Asia/Kolkata'
AS 
COPY INTO  raw_aqi
( index_record_ts, json_data, record_count, json_version, _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts )

FROM
 (
    SELECT
    TRY_TO_TIMESTAMP(t.$1:records[0].last_update::text , 'dd-mm-yyyy hh24:mi:ss'  ) AS index_record_ts,
    t.$1,
    t.$1:total::int AS record_count,
    t.$1:version::text AS json_version,
    -- meta data information
    metadata$filename AS _stg_file_name,
    metadata$FILE_LAST_MODIFIED AS _stg_file_load_ts,
    metadata$FILE_CONTENT_KEY AS _stg_file_md5,
    CURRENT_TIMESTAMP() AS _copy_data_ts
    
FROM 
    @dev_db.stage_sch.raw_stg AS t    
)
FILE_FORMAT = ( FORMAT_NAME = 'dev_db.stage_sch.JSON_FILE_FORMAT' )
ON_ERROR = ABORT_STATEMENT;

---------------------------------------------------
--Convert Task from suspended to started state
SHOW TASKS;
USE ROLE accountadmin;
GRANT EXECUTE TASK, EXECUTE MANAGED TASK ON ACCOUNT TO ROLE sysadmin; 
USE ROLE sysadmin;

ALTER TASK dev_db.stage_sch.copy_air_quality_data resume;
SHOW TASKS;
---------------------------------------------------

SELECT * FROM raw_aqi LIMIT 10;

