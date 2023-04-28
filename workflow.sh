#! /bin/bash

# Variables
RUN_DATE=$(date +"%Y-%m-%d")
FILE_NAME_SUFFIX=$(date +"%Y%m%d%s")

PROJECT_ID=$1
GCS_BUCKET=$2
GCS_DATA_PATH=${GCS_BUCKET}/data
GCS_ARCHIVE_PATH=${GCS_BUCKET}/archive
GCS_OUTPUT_PATH=${GCS_BUCKET}/output
GCS_STAGING_PATH=${GCS_BUCKET}/staging

LOCAL_OUTPUT_PATH=${PWD}/output/
mkdir -p ${LOCAL_OUTPUT_PATH}

# Create log file
LOG_PATH=${PWD}/logs/
mkdir -p ${LOG_PATH}

LOG_FILE=${LOG_PATH}/bq_etl_job_${FILE_NAME_SUFFIX}.log
exec > $LOG_FILE


# STEP 1: Upload data to GCS
mkdir -p ${PWD}/data/
curl https://raw.githubusercontent.com/progrkaushl/datasets/master/csv/office_data/OfficeData.csv -o ${PWD}/data/OfficeData.csv
curl https://raw.githubusercontent.com/progrkaushl/datasets/master/csv/office_data/OfficeDataProject.csv -o ${PWD}/data/OfficeDataProject.csv

gsutil cp ${PWD}/data/OfficeData.csv ${GCS_DATA_PATH}/office_data/OfficeData.csv
gsutil cp ${PWD}/data/OfficeDataProject.csv ${GCS_DATA_PATH}/office_data/OfficeDataProject.csv


# STEP 1: Create TABLE
bq --location=US mk --dataset \
    --default_table_expiration 3600 \
    --description "Dataset for storing tables." \
    ${PROJECT_ID}.office_db

create_table_command="
CREATE TABLE IF NOT EXISTS ${PROJECT_ID}.office_db.office_data (
employee_name STRING, 
department STRING, 
state STRING, 
salary INT64, 
age INT64, bonus INT64
);

CREATE TABLE IF NOT EXISTS ${PROJECT_ID}.office_db.office_data_projects (
employee_id INT64, 
employee_name STRING, 
department STRING, 
state STRING, 
salary INT64, 
age INT64, 
bonus INT64
);
"
echo $create_table_command
bq query --nouse_legacy_sql $create_table_command


# STEP 2: Load data from GCS into table
load_data_command="
LOAD DATA OVERWRITE ${PROJECT_ID}.office_db.office_data
FROM FILES (
format='CSV',
field_delimiter=',',
uris=['${GCS_DATA_PATH}/office_data/*']
);

LOAD DATA OVERWRITE ${PROJECT_ID}.office_db.office_data
FROM FILES (
format='CSV',
field_delimiter=',',
uris=['${GCS_DATA_PATH}/office_data/*']
);
"
echo $load_data_command
bq query --nouse_legacy_sql $load_data_command


# STEP 3: Do join & export data in hive partitioning way
export_sales_data_command="
EXPORT DATA OPTIONS(
  uri='${GCS_STAGING_PATH}/office_data/*',
  format='CSV',
  overwrite=true,
  header=false,
  field_delimiter='|'
) AS 
SELECT 
  p.employee_id, 
  p.employee_name, 
  p.state, 
  p.salary, 
  p.age, 
  p.bonus
FROM ${PROJECT_ID}.office_db.office_data_projects p
WHERE p.department = 'Sales'
"
echo $export_sales_data_command
bq query --nouse_legacy_sql export_sales_data_command


# STEP 4: Add header to the file
rm -r ${LOCAL_OUTPUT_PATH}/office_data/

echo "Copy and merge files from GCS to Local"
gsutil cat ${GCS_STAGING_PATH}/office_data/* >> ${LOCAL_OUTPUT_PATH}/office_data/office_data_sales.txt


# STEP 6: Copy file to Destination folder in GCS
echo "Copy file into GCS for Hive Partitioned Table..."
gsutil cp ${LOCAL_OUTPUT_PATH}/office_data/office_data_sales.txt ${GCS_OUTPUT_PATH}/office_data/date_dt=${RUN_DATE}/office_data_sales_${FILE_NAME_SUFFIX}.txt

# STEP 7: Create final hive partitioned table
create_final_table_command="
CREATE EXTERNAL TABLE IF NOT EXISTS ${PROJECT_ID}.office_db.office_data_projects_hive_partitioned (
employee_id INT64, 
employee_name STRING, 
state STRING, 
salary INT64, 
age INT64, 
bonus INT64
)
WITH PARTITION COLUMNS
OPTIONS (
format='CSV',
field_delimiter='|',
hive_partition_uri_prefix='${GCS_OUTPUT_PATH}/office_data/',
uris=['${GCS_OUTPUT_PATH}/office_data/*']
);
"
echo $create_final_table_command
bq query --nouse_legacy_sql $create_final_table_command


