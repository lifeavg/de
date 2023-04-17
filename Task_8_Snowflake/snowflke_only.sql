
CREATE WAREHOUSE IF NOT EXISTS TEST_WH;

USE WAREHOUSE TEST_WH;

CREATE DATABASE IF NOT EXISTS TEST_DB;

CREATE SCHEMA TEST_DB.TEST_SH;

CREATE FILE FORMAT IF NOT EXISTS TEST_DB.TEST_SH.IOS_APPS_INFO_FILE
TYPE=CSV
SKIP_HEADER=1
FIELD_OPTIONALLY_ENCLOSED_BY='"'
SKIP_BLANK_LINES = TRUE;

CREATE TABLE IF NOT EXISTS TEST_DB.TEST_SH.RAW_TABLE (
    id CHAR(24) PRIMARY KEY,
    ios_app_id INTEGER,
    title VARCHAR(256),
    developer_name VARCHAR(256),
    developer_ios_id INTEGER,
    ios_store_url VARCHAR(2048),
    seller_official_website VARCHAR(2048),
    age_rating VARCHAR(16),
    total_average_rating NUMBER(2,1),
    total_number_of_ratings INTEGER,
    average_rating_for_version NUMBER(38,1),
    number_of_ratings_for_version INTEGER,
    original_release_date VARCHAR(20),
    current_version_release_date VARCHAR(20),
    price_usd NUMBER(10,2),
    primary_genre VARCHAR(22),
    all_genres VARCHAR(200),
    languages VARCHAR(1000),
    description VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS TEST_DB.TEST_SH.STAGE_TABLE (
    id CHAR(24) PRIMARY KEY,
    ios_app_id INTEGER,
    title VARCHAR(256),
    developer_name VARCHAR(256),
    developer_ios_id INTEGER,
    ios_store_url VARCHAR(2048),
    seller_official_website VARCHAR(2048),
    age_rating VARCHAR(16),
    total_average_rating NUMBER(2,1),
    total_number_of_ratings INTEGER,
    average_rating_for_version NUMBER(38,1),
    number_of_ratings_for_version INTEGER,
    original_release_date VARCHAR(20),
    current_version_release_date VARCHAR(20),
    price_usd NUMBER(10,2),
    primary_genre VARCHAR(22),
    all_genres VARCHAR(200),
    languages VARCHAR(1000),
    description VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS TEST_DB.TEST_SH.MASTER_TABLE (
    id CHAR(24) PRIMARY KEY,
    ios_app_id INTEGER,
    title VARCHAR(256),
    developer_name VARCHAR(256),
    developer_ios_id INTEGER,
    ios_store_url VARCHAR(2048),
    seller_official_website VARCHAR(2048),
    age_rating VARCHAR(16),
    total_average_rating NUMBER(2,1),
    total_number_of_ratings INTEGER,
    average_rating_for_version NUMBER(38,1),
    number_of_ratings_for_version INTEGER,
    original_release_date VARCHAR(20),
    current_version_release_date VARCHAR(20),
    price_usd NUMBER(10,2),
    primary_genre VARCHAR(22),
    all_genres VARCHAR(200),
    languages VARCHAR(1000),
    description VARCHAR(4000)
);

CREATE OR REPLACE STREAM TEST_DB.TEST_SH.RAW_STREAM ON TABLE RAW_TABLE;

CREATE OR REPLACE STREAM TEST_DB.TEST_SH.STAGE_STREAM ON TABLE STAGE_TABLE;

CREATE TASK IF NOT EXISTS TEST_DB.TEST_SH.RAW_TASK
WHEN SYSTEM$STREAM_HAS_DATA(TEST_DB.TEST_SH.RAW_STREAM)
AS
MERGE INTO TEST_DB.TEST_SH.STAGE_TABLE ST
USING TEST_DB.TEST_SH.RAW_STREAM RS
ON ST.id = RS.id
  WHEN MATCHED AND metadata$action = 'DELETE' AND metadata$isupdate = 'FALSE'
    THEN DELETE
  WHEN MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'TRUE'
    THEN UPDATE SET ST.id = RS.id,
                    ST.ios_app_id = RS.ios_app_id,
                    ST.title = RS.title,
                    ST.developer_name = RS.developer_name,
                    ST.developer_ios_id = RS.developer_ios_id,
                    ST.ios_store_url = RS.ios_store_url,
                    ST.seller_official_website = RS.seller_official_website,
                    ST.age_rating = RS.age_rating,
                    ST.total_average_rating = RS.total_average_rating,
                    ST.total_number_of_ratings = RS.total_number_of_ratings,
                    ST.average_rating_for_version = RS.average_rating_for_version,
                    ST.number_of_ratings_for_version = RS.number_of_ratings_for_version,
                    ST.original_release_date = RS.original_release_date,
                    ST.current_version_release_date = RS.current_version_release_date,
                    ST.price_usd = RS.price_usd,
                    ST.primary_genre = RS.primary_genre,
                    ST.all_genres = RS.all_genres,
                    ST.languages = RS.languages,
                    ST.description = RS.description
  WHEN NOT MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'FALSE'
    THEN INSERT (id,
                 ios_app_id,
                 title,
                 developer_name,
                 developer_ios_id,
                 ios_store_url,
                 seller_official_website,
                 age_rating,
                 total_average_rating,
                 total_number_of_ratings,
                 average_rating_for_version,
                 number_of_ratings_for_version,
                 original_release_date,
                 current_version_release_date,
                 price_usd,
                 primary_genre,
                 all_genres,
                 languages,
                 description)
         VALUES (RS.id,
                 RS.ios_app_id,
                 RS.title,
                 RS.developer_name,
                 RS.developer_ios_id,
                 RS.ios_store_url,
                 RS.seller_official_website,
                 RS.age_rating,
                 RS.total_average_rating,
                 RS.total_number_of_ratings,
                 RS.average_rating_for_version,
                 RS.number_of_ratings_for_version,
                 RS.original_release_date,
                 RS.current_version_release_date,
                 RS.price_usd,
                 RS.primary_genre,
                 RS.all_genres,
                 RS.languages,
                 RS.description);

CREATE TASK IF NOT EXISTS TEST_DB.TEST_SH.STAGE_TASK
AFTER TEST_DB.TEST_SH.RAW_TASK
AS
MERGE INTO TEST_DB.TEST_SH.MASTER_TABLE MT
USING TEST_DB.TEST_SH.STAGE_STREAM SS
ON MT.id = SS.id
  WHEN MATCHED AND metadata$action = 'DELETE' AND metadata$isupdate = 'FALSE'
    THEN DELETE
  WHEN MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'TRUE'
    THEN UPDATE SET MT.id = SS.id,
                    MT.ios_app_id = SS.ios_app_id,
                    MT.title = SS.title,
                    MT.developer_name = SS.developer_name,
                    MT.developer_ios_id = SS.developer_ios_id,
                    MT.ios_store_url = SS.ios_store_url,
                    MT.seller_official_website = SS.seller_official_website,
                    MT.age_rating = SS.age_rating,
                    MT.total_average_rating = SS.total_average_rating,
                    MT.total_number_of_ratings = SS.total_number_of_ratings,
                    MT.average_rating_for_version = SS.average_rating_for_version,
                    MT.number_of_ratings_for_version = SS.number_of_ratings_for_version,
                    MT.original_release_date = SS.original_release_date,
                    MT.current_version_release_date = SS.current_version_release_date,
                    MT.price_usd = SS.price_usd,
                    MT.primary_genre = SS.primary_genre,
                    MT.all_genres = SS.all_genres,
                    MT.languages = SS.languages,
                    MT.description = SS.description
  WHEN NOT MATCHED AND metadata$action = 'INSERT' AND metadata$isupdate = 'FALSE'
    THEN INSERT (id,
                 ios_app_id,
                 title,
                 developer_name,
                 developer_ios_id,
                 ios_store_url,
                 seller_official_website,
                 age_rating,
                 total_average_rating,
                 total_number_of_ratings,
                 average_rating_for_version,
                 number_of_ratings_for_version,
                 original_release_date,
                 current_version_release_date,
                 price_usd,
                 primary_genre,
                 all_genres,
                 languages,
                 description)
          VALUES (SS.id,
                  SS.ios_app_id,
                  SS.title,
                  SS.developer_name,
                  SS.developer_ios_id,
                  SS.ios_store_url,
                  SS.seller_official_website,
                  SS.age_rating,
                  SS.total_average_rating,
                  SS.total_number_of_ratings,
                  SS.average_rating_for_version,
                  SS.number_of_ratings_for_version,
                  SS.original_release_date,
                  SS.current_version_release_date,
                  SS.price_usd,
                  SS.primary_genre,
                  SS.all_genres,
                  SS.languages,
                  SS.description);

ALTER TASK TEST_DB.TEST_SH.RAW_TASK RESUME;

ALTER TASK TEST_DB.TEST_SH.STAGE_TASK RESUME;


PUT file:///home/lifeavg/Documents/projects/de/Task_8_Snowflake/data/763K_plus_IOS_Apps_Info_cut.csv @~/staged;

COPY INTO TEST_DB.TEST_SH.RAW_TABLE
    FROM @~/staged
    PATTERN = '.*763K_plus_IOS_Apps_Info.*'
    FILE_FORMAT = (FORMAT_NAME = TEST_DB.TEST_SH.IOS_APPS_INFO_FILE);
