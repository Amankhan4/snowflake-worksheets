Create warehouse AWS_WH;
create database DBT_DB;
create schema DBT_DB.Try;

---  -----  ----   -----

-- Create storage object this will help us to connect to aws.

create or replace storage integration Snow_OBJ
type = external_stage
storage_provider = s3
enabled = True
storage_aws_role_arn = 'arn:aws:iam::390844748718:role/Data-transfer-role'
storage_allowed_locations = ('s3://snowflake-datetransfer/');

desc integration Snow_OBJ;

-- Create a File Formate

create or replace file format csv_format type = csv field_delimiter ="," skip_header = 1 null_if = ('NULL','null') empty_field_as_null = true;

-- Create a stage. A stage specifies where data file are stored so that data in file can be loaded into an table.

create or replace stage snow_stage_2024
storage_integration = Snow_OBJ
url = 's3://snowflake-datetransfer/'
file_format = csv_format;

-- Create a table to storethe data 

create or replace table data
(
    Duration int,
    Date varchar(20),
    Pulse int,
    Maxpulse int,
    Calories int
)

-- Copy into table

copy into data from @snow_stage_2024
ON_ERROR ='skip_file';

select * from try.data;

-- Cleaning and setting proper data type to the date column as it incudes strings.
alter table data add column new_date date;

UPDATE try.data
SET new_date = TRY_TO_DATE(REPLACE(date, '''', ''), 'YYYY/MM/DD');

alter table data drop column date;

alter table data rename column new_date to date;




