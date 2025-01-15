-- Create a warehouse
CREATE WAREHOUSE AWS_WH;

-- Create a database and schema
CREATE DATABASE DBT_DB;
CREATE SCHEMA DBT_DB.Try;

-- Create storage integration to connect to AWS
CREATE OR REPLACE STORAGE INTEGRATION Snow_OBJ
    TYPE = external_stage
    STORAGE_PROVIDER = s3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::390844748718:role/Data-transfer-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-datetransfer/');

DESC INTEGRATION Snow_OBJ;

-- Create a file format for CSV files
CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = csv
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE;

-- Create a stage specifying where data files are stored
CREATE OR REPLACE STAGE snow_stage_2024
    STORAGE_INTEGRATION = Snow_OBJ
    URL = 's3://snowflake-datetransfer/'
    FILE_FORMAT = csv_format;

-- Create a table to store the data
CREATE OR REPLACE TABLE data (
    Duration INT,
    Date VARCHAR(20),
    Pulse INT,
    Maxpulse INT,
    Calories INT
);

-- Copy data into the table
COPY INTO data
FROM @snow_stage_2024
ON_ERROR = 'skip_file';

-- Select all data from the table
SELECT * FROM try.data;

-- Cleaning and setting proper data type for the date column
ALTER TABLE data ADD COLUMN new_date DATE;

UPDATE try.data
SET new_date = TRY_TO_DATE(REPLACE(Date, '''', ''), 'YYYY/MM/DD');

ALTER TABLE data DROP COLUMN Date;
ALTER TABLE data RENAME COLUMN new_date TO Date;

