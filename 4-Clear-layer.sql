-- change the context
use role sysadmin;
use schema dev_db.clean_sch;
use warehouse adhoc_wh;

-- Ste-1
select * from dev_db.stage_sch.raw_aqi order by id;
-- Step-2
select id, index_record_ts, from dev_db.stage_sch.raw_aqi order by id;

-- Step-3
select 
    id, index_record_ts
from 
    dev_db.stage_sch.raw_aqi 
where 
    index_record_ts is not null
order by id;

-- Step-4
select 
    id,
    index_record_ts,
    json_data,
    record_count,
    json_version,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5 ,
    _copy_data_ts
from 
    dev_db.stage_sch.raw_aqi 
where 
    index_record_ts is not null
    limit 5;

-- Step 5 query without JSON data column
select 
    id,
    index_record_ts,
    record_count,
    json_version,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5 ,
    _copy_data_ts
from 
    dev_db.stage_sch.raw_aqi 
where 
    index_record_ts is not null; -- this will give all 24 records

-- now lets loads some duplicate data that is a common issue in some of the 
-- data project and validate the scenario 

select 
    id,
    index_record_ts,
    record_count,
    json_version,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5 ,
    _copy_data_ts,
    row_number() over (partition by index_record_ts order by _stg_file_load_ts desc) as latest_file_rank
from 
    dev_db.stage_sch.raw_aqi 
where 
    index_record_ts is not null; -- this will give all 24 records


    

-- de-duplication of the records + flattening it
with air_quality_with_rank as (
    select 
        index_record_ts,
        json_data,
        record_count,
        json_version,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5 ,
        _copy_data_ts,
        row_number() over (partition by index_record_ts order by _stg_file_load_ts desc) as latest_file_rank
    from dev_db.stage_sch.raw_aqi
    where index_record_ts is not null
),
unique_air_quality_data as (
    select 
        * 
    from 
        air_quality_with_rank 
    where latest_file_rank = 1
)
    select 
        index_record_ts,
        hourly_rec.value:country::text as country,
        hourly_rec.value:state::text as state,
        hourly_rec.value:city::text as city,
        hourly_rec.value:station::text as station,
        hourly_rec.value:latitude::number(12,7) as latitude,
        hourly_rec.value:longitude::number(12,7) as longitude,
        hourly_rec.value:pollutant_id::text as pollutant_id,
        hourly_rec.value:pollutant_max::text as pollutant_max,
        hourly_rec.value:pollutant_min::text as pollutant_min,
        hourly_rec.value:pollutant_avg::text as pollutant_avg,

        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
  from 
    unique_air_quality_data ,
    lateral flatten (input => json_data:records) hourly_rec;


-- creating dynamic table
--Tells Snowflake to refresh when upstream data changes (event-driven)

-- TARGET_LAG= '5 MINUTES', '1 HOUR', '1 DAY' ,ON CHANGE (limited preview), DOWNSTREAM


CREATE OR REPLACE DYNAMIC TABLE clean_aqi_dt
    TARGET_LAG= 'DOWNSTREAM'
    WAREHOUSE= Transform_wh
AS 
   WITH air_quality_with_rank AS (
   SELECT 
        index_record_ts,
        json_data,
        record_count,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts,
        ROW_NUMBER() OVER( PARTITION BY index_record_ts ORDER BY _stg_file_load_ts DESC ) AS latest_file_rank
   FROM dev_db.stage_sch.raw_aqi
   WHERE index_record_ts IS NOT NULL
   ) ,
   unique_air_quality_data AS (
   SELECT *
   FROM 
        air_quality_with_rank
    WHERE latest_file_rank = 1
)   
    SELECT 
        index_record_ts,
        hourly_rec.value:country::text AS country,
        hourly_rec.value:state::text AS state,
        hourly_rec.value:city::text AS city,
        hourly_rec.value:station::text AS station,
        hourly_rec.value:latitude::number(12,7) AS latitude,
        hourly_rec.value:longitude::number(12,7) AS longitude,
        hourly_rec.value:pollutant_id::text AS pollutant_id,
        hourly_rec.value:pollutant_max::text AS pollutant_max,
        hourly_rec.value:pollutant_min::text AS pullutant_min,
        hourly_rec.value:pollutant_avg::text AS pollutant_avg,

        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    FROM 
        unique_air_quality_data ,
        LATERAL FLATTEN( input => json_data:records ) hourly_rec; 
        

SELECT * FROM clean_aqi_dt limit 10;        
