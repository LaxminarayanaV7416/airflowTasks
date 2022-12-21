import json
import os
import paramiko
import boto3
import time
from paramiko.client import SSHClient
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable

DEFAULT_ARGS={
        'depends_on_past': False,
        'email': ['laxminarayana.vadnala@talentinc.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    }

def trigger_glue(ti):
    session = boto3.session.Session(aws_access_key_id = Variable.get("TalentStagingAccessKey"),
                               aws_secret_access_key =Variable.get("TalentStagingSecretKey"),
                               region_name = Variable.get("TalentStagingRegionName"))
    glue = session.client("glue")
    while True:
        try:
            res = glue.start_job_run(JobName = "upserts_check")
            print(f"hurrey job have started with job id - {res['JobRunId']}")
            ti.xcom_push(key = 'JobRunId', value = res['JobRunId'])
            break
        except Exception as err:
            time.sleep(60)
    return True

def get_glue_job_status(ti):
    session = boto3.session.Session(aws_access_key_id = Variable.get("TalentStagingAccessKey"),
                               aws_secret_access_key =Variable.get("TalentStagingSecretKey"),
                               region_name = Variable.get("TalentStagingRegionName"))
    glue = session.client("glue")
    while True:
        time.sleep(10)
        status_detail = glue.get_job_run(JobName="upserts_check", 
                            RunId = ti.xcom_pull(task_ids = "triggerGlueJob",key = "JobRunId"))
        status = status_detail.get("JobRun").get("JobRunState")
        if status in ["FAILED", "ERROR", "TIMEOUT", "WAITING"]:
            raise Exception(f'Glue job run with id {ti.xcom_pull(task_ids = "triggerGlueJob",key = "JobRunId")} failed')
        elif status in ["SUCCEEDED"]:
            return True
        elif status in ["STOPPED", "STOPPING"]:
            raise Exception(f'Glue job run with id {ti.xcom_pull(task_ids = "triggerGlueJob",key = "JobRunId")} stopping')



with DAG('UpsertFromAirflowDemoApp',
    default_args=DEFAULT_ARGS,
    description='this Dag will upsert the data to the existing hudi table',
    # schedule=timedelta(days=
    # 1),
    start_date=datetime(2022, 10, 5),
    catchup=False,
    tags=['startwire']):

    step_3 = PythonOperator(
        task_id = "triggerGlueJob",
        python_callable = trigger_glue
    )

    step_4 = PythonOperator(
        task_id = "glueJobStatus",
        python_callable = get_glue_job_status
    )


step_3 >> step_4