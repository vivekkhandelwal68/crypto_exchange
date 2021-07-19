create or replace storage integration s3_int
type = external_stage
storage_provider = s3
storage_aws_role_arn = <storage_aws_role_arn>
enabled = TRUE
storage_allowed_locations = ('s3://<BUCKET_NAME>/');


create or replace stage s3_stage
URL='s3://<BUCKET_NAME'
STORAGE_INTEGRATION=S3_INT
;

CREATE TABLE COINS_MAIN_DIM (
PRIMARY_ID INT IDENTITY START 1 INCREMENT 1 ,
ID VARCHAR,
NAME VARCHAR,
SYMBOL VARCHAR
);

CREATE OR REPLACE TABLE EXCHANGE_STG(
RAW_DATA VARIANT,
FILE_NAME VARCHAR,
LOAD_DATE TIMESTAMP
);

create table exchange_fact(
PRIMARY_ID INT IDENTITY START 1 INCREMENT 1 ,
COIN_ID INT,
BTC_VALUE DOUBLE,
LOAD_DATE TIMESTAMP
);

create OR REPLACE stream exchange_stream ON TABLE EXCHANGE_STG
APPEND_ONLY = TRUE ;

CREATE OR REPLACE TASK EXCHANGE_LOAD_TASK
  WAREHOUSE = COMMON_WH
  SCHEDULE = '2 minute'
WHEN
  SYSTEM$STREAM_HAS_DATA('exchange_stream')
AS
  INSERT INTO exchange_fact(COIN_ID, BTC_VALUE, LOAD_DATE)
  WITH MAIN_CTE AS(
SELECT value:name::STRING AS NAME, value:type::STRING AS TYPE, value:unit::STRING AS UNIT,  value:value::DOUBLE AS BTC_VALUE, LOAD_DATE FROM exchange_stream, LATERAL FLATTEN(input => exchange_stream.RAW_DATA) WHERE TYPE='crypto'
)SELECT PRIMARY_ID AS COIN_ID, BTC_VALUE, LOAD_DATE FROM MAIN_CTE JOIN COINS_MAIN_DIM ON UPPER(MAIN_CTE.NAME)=UPPER(COINS_MAIN_DIM.NAME);

alter task EXCHANGE_LOAD_TASK resume;


CREATE OR REPLACE VIEW AVG_COIN_PRICE_BY_DAYNO AS(
select NAME, DAYOFYEAR(LOAD_DATE) AS DAYNO, AVG(BTC_VALUE) AS BTC_VALUE from exchange_fact ef join COINS_MAIN_DIM CMD ON ef.COIN_ID=CMD.PRIMARY_ID group by NAME,DAYOFYEAR(LOAD_DATE)
);

//Read data;
select * from EXCHANGE_STG;

select * FROM exchange_stream;

select * from exchange_fact;

truncate table exchange_fact;

select * from AVG_COIN_PRICE_BY_DAYNO ORDER BY DAYNO DESC;
