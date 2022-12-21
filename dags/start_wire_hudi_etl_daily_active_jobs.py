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


def trigger_windows_dos_command_on_ec2(command, start_date = None):
    if start_date is not None:
        command = f"{command} {start_date}"
    ssh_client = SSHClient()
    HOST = Variable.get("StartWireVPNEc2InstanceURL")
    USERNAME = Variable.get("StartWireVPNEc2InstanceUserName")
    PASSWORD = Variable.get("StartWireVPNEc2InstancePassword")
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(HOST, username = USERNAME, password = PASSWORD)
    x,y,z = ssh_client.exec_command(command, timeout = 50)
    try:
        success_data = y.read().decode("utf-8")
        failure_data = z.read().decode("utf-8")
        print(f"Successfully command execute the output is, {success_data}")
        if len(failure_data)>0:
            print(f"task failed due to {failure_data}")
            raise Exception("paramiko failed to execute ec2 isntane command")
        else:
            ssh_client.close()
            return success_data
    except Exception as err:
        return str(err)

def trigger_glue(start_date, ti):
    session = boto3.session.Session(aws_access_key_id = Variable.get("TalentStagingAccessKey"),
                               aws_secret_access_key =Variable.get("TalentStagingSecretKey"),
                               region_name = Variable.get("TalentStagingRegionName"))
    start_date = start_date.replace("-", "/")
    glue = session.client("glue")
    while True:
        try:
            res = glue.start_job_run(JobName = "startWireUpsertsDailyActiveJobs", Arguments = {"--start_date" : start_date})
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
        status_detail = glue.get_job_run(JobName="startWireUpsertsDailyActiveJobs", 
                            RunId = ti.xcom_pull(task_ids = "triggerGlueJob",key = "JobRunId"))
        status = status_detail.get("JobRun").get("JobRunState")
        if status in ["FAILED", "ERROR", "TIMEOUT", "WAITING"]:
            raise Exception(f'Glue job run with id {ti.xcom_pull(task_ids = "triggerGlueJob",key = "JobRunId")} failed')
        elif status in ["SUCCEEDED"]:
            return True
        elif status in ["STOPPED", "STOPPING"]:
            raise Exception(f'Glue job run with id {ti.xcom_pull(task_ids = "triggerGlueJob",key = "JobRunId")} stopping')



DEFAULT_ARGS={
        'depends_on_past': False,
        'email': ['laxminarayana.vadnala@talentinc.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    }

STATUS_COMMAND = f"C:/ProgramData/Anaconda3/python.exe check_status.py --start_date"
TRIGGER_GLUE_COMMAND = f"C:/ProgramData/Anaconda3/python.exe daily_run.py --start_date"

def check_status_contionusly(command,start_date):
    STATUS_COMMAND = f"{command} {start_date}"
    while True:
        try:
            status = trigger_windows_dos_command_on_ec2(STATUS_COMMAND)
            status = status.strip("\r\n")
            status = status.strip("\n")
            status = status.strip("\r")
            if status in ["DONE", "FAILED"]:
                print(f"status of the command is returned finally as {status}")
                break
            else:
                print(f"status is still running currently it is {status}")
        except Exception as err:
            print(err)
            time.sleep(10)
    if status == 'DONE':
        return status
    else:
        raise Exception(f"failed with status as {status}")


with DAG('StartWireHudiUpsertsDailyActiveJobs',
    default_args=DEFAULT_ARGS,
    description='this Dag will upsert the data to the existing hudi table',
    schedule=timedelta(days=
    1),
    start_date=datetime(2022, 10, 5),
    catchup=True,
    tags=['startwire']):

    step_1 = PythonOperator(
        task_id = "hitGlueJob",
        python_callable = trigger_windows_dos_command_on_ec2,
        op_kwargs = {"command" : TRIGGER_GLUE_COMMAND, "start_date" : "{{ ds }}"}
    )

    step_2 = PythonOperator(
        task_id = "infintelyCheckWhetherItsCompleted",
        python_callable = check_status_contionusly,
        op_kwargs = {"command" : STATUS_COMMAND,"start_date" : "{{ ds }}"}
    )

    step_3 = PythonOperator(
        task_id = "triggerGlueJob",
        python_callable = trigger_glue,
        op_kwargs = {"start_date" : "{{ ds }}"}
    )

    step_4 = PythonOperator(
        task_id = "glueJobStatus",
        python_callable = get_glue_job_status
    )


step_1 >> step_2 >> step_3 >> step_4
    
    