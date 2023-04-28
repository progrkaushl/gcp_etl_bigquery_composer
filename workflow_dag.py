from datetime import datetime, timedelta
import os
import logging 
import yaml
from airflow import models
from airflow import DAG 
from airflow.contrib.operators import bigquery_to_gcs, gcs_to_bq, gcs_to_gcs
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator


# Get parameter values from configuration.yaml file
with open('configuration.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file.read())

PROJECT_ID = config['config_params']['project_params']['project_id']
SERVICE_ACCOUNT = config['config_params']['project_params']['service_account']
EMAIL_ACCOUNT = config['config_params']['project_params']['email_account']
INSTANCE_ZONE = config['config_params']['gce_params']['instance_zone']
INSTANCE_MACHINE_TYPE = config['config_params']['gce_params']['machine_type']
INSTANCE_NAME = config['config_params']['gce_params']['instance_name']
GCS_BUCKET = config['config_params']['gcs_params']['gcs_bucket']
GCS_SCRIPT_PATH = config['config_params']['gcs_params']['gcs_script_path']


# Define dag arguments to pass in default_args
yesterday = datetime.combine(
    datetime.today() - timedelta(1), 
    datetime.min.time()
)

default_dag_args = {
    'start_date': yesterday,
    'email': EMAIL_ACCOUNT,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'project_id': PROJECT_ID
}


# Command to create a GCE Instance
instance_create_command = """
    gcloud compute instances create {INSTANCE_NAME} \
    --project={PROJECT_ID} \
    --zone={INSTANCE_ZONE} \
    --machine-type={INSTANCE_MACHINE_TYPE} \
    --network-interface=network-tier=PREMIUM,stack-type=IPV4_ONLY,subnet=default \
    --maintenance-policy=MIGRATE \
    --provisioning-model=STANDARD \
    --service-account={SERVICE_ACCOUNT} \
    --scopes=https://www.googleapis.com/auth/cloud-platform \
    --create-disk=auto-delete=yes,boot=yes,device-name={INSTANCE_NAME},image=projects/debian-cloud/global/images/debian-11-bullseye-v20230411,mode=rw,size=10,type=projects/{PROJECT_ID}/zones/{INSTANCE_ZONE}/diskTypes/pd-balanced \
    --no-shielded-secure-boot \
    --shielded-vtpm \
    --shielded-integrity-monitoring \
    --labels=ec-src=vm_add-gcloud \
    --reservation-affinity=any
""".format(
    INSTANCE_NAME = INSTANCE_NAME, 
    PROJECT_ID = PROJECT_ID, 
    INSTANCE_ZONE = INSTANCE_ZONE,
    INSTANCE_MACHINE_TYPE = INSTANCE_MACHINE_TYPE,
    SERVICE_ACCOUNT = SERVICE_ACCOUNT
)


# Command to copy and execute workflow.sh on GCE Instance
workflow_execute_command = """
    gcloud compute --project {PROJECT_ID} ssh {INSTANCE_NAME} --internal-ip --zone {INSTANCE_ZONE} \
    --command "mkdir etl_home && cd etl_home && gsutil cp {GCS_SCRIPT_PATH}/workflow.sh . && sh workflow.sh ${PROJECT_ID} ${GCS_BUCKET}"
""".format(
    INSTANCE_NAME = INSTANCE_NAME,
    PROJECT_ID = PROJECT_ID,
    INSTANCE_ZONE = INSTANCE_ZONE,
    GCS_SCRIPT_PATH = GCS_SCRIPT_PATH,
    GCS_BUCKET = GCS_BUCKET
)


# Command to delete a GCE Instance
instance_delete_command = """
    gcloud compute instances delete {INSTANCE_NAME} --zone={INSTANCE_ZONE}
""".format(
    INSTANCE_NAME = INSTANCE_NAME,
    INSTANCE_ZONE = INSTANCE_ZONE
)


# Airflow DAG
with DAG(
    dag_id = "gcp_bigquery_etl",
    schedule_interval="0 4 * * *",
    default_args = default_dag_args
) as dag:
    
    start = DummyOperator(task_id = 'start')
    
    bash_create_instance = BashOperator(
        task_id = 'bash_create_instance_task',
        bash_command = instance_create_command
    )
    
    bash_workflow_execute = BashOperator(
        task_id = 'bash_workflow_execute_task',
        bash_command = workflow_execute_command
    )
    
    bash_delete_instance = BashOperator(
        task_id = 'bash_delete_instance_task',
        bash_command = instance_delete_command
    )
  
    end = DummyOperator(task_id = 'end')
  
 start >> bash_create_instance >> bash_workflow_execute >> bash_delete_instance >> end
