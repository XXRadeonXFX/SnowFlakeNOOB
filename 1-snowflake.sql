USE ROLE sysadmin;


SHOW WAREHOUSES ;

USE WAREHOUSE compute_wh;

CREATE DATABASE leetcode;

CREATE STAGE stage_location;


SHOW Stages;


CREATE OR REPLACE FILE FORMAT my_json_ff
TYPE = 'JSON';


LIST @stage_location;


SELECT t.$1 FROM @leetcode.public.stage_location ( file_format => my_json_ff ) t;

SELECT 
t.$1:total::int AS  record_count
,t.$1:count::int AS  count

--FROM record KEY
,t.$1:records[1].last_update::text AS record_ts

-- Station Detail
,t.$1:records[1].country::text AS country
,t.$1:records[1].state::text AS state
,t.$1:records[1].city::text AS city
,t.$1:records[1].station::text AS station
,t.$1:records[1].latitude::text AS latitude
,t.$1:records[1].longitude::text AS longitude



--Pollution Detail
,t.$1:records[1].pollutant_id::text AS pollution_id
,t.$1:records[1].pollutant_min::text AS pollution_min
,t.$1:records[1].pollutant_max::text AS pollution_max
,t.$1:records[1].pollutant_avg::text AS pollution_avg 
FROM @leetcode.public.stage_location ( file_format => my_json_ff ) t;




