from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret

from datetime import datetime, timedelta
import uuid

##############################################################################
# Generic Airflow dag arguments
DAG_STARTDATE = datetime(2020, 1, 31, 00)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': DAG_STARTDATE,
    'email': ['riaz.arbi@capetown.gov.za'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# env variables for inside the k8s pod
k8s_run_env = {
    'CODE_REPO': 'covid-19-data',
    'DB_UTILS_DIR': '/tmp/db-utils',
    'SECRETS_FILE': '/secrets/secrets.json',
    'DB_UTILS_LOCATION': 'https://ds2.capetown.gov.za/db-utils',
    'DB_UTILS_PKG': 'db_utils-0.3.2-py2.py3-none-any.whl'
}

# airflow-workers' secrets
secret_file = Secret('volume', '/secrets', 'airflow-workers-secret')

# arguments for the k8s operators
k8s_py_run_args = {
    "image": "cityofcapetown/datascience:python",
    "namespace": 'airflow-workers',
    "is_delete_operator_pod": True,
    "get_logs": True,
    "in_cluster": True,
    "secrets": [secret_file],
    "env_vars": k8s_run_env,
    "image_pull_policy": "IfNotPresent",
    "startup_timeout_seconds": 60*30,
    "task_concurrency": 1,
}

k8s_r_run_args = {
    "image": "riazarbi/datasci-r-heavy:latest",
    "namespace": 'airflow-workers',
    "is_delete_operator_pod": True,
    "get_logs": True,
    "in_cluster": True,
    "secrets": [secret_file],
    "env_vars": k8s_run_env,
    "image_pull_policy": "IfNotPresent",
    "startup_timeout_seconds": 60*30,
    "task_concurrency": 1,
}


##############################################################################
# Generic prod repo arguments
startup_cmd = 'git clone https://ds1.capetown.gov.za/ds_gitlab/OPM/covid-19-data.git "$CODE_REPO" && ' \
              'pip3 install $DB_UTILS_LOCATION/$DB_UTILS_PKG && ' \
              'git clone https://ds1.capetown.gov.za/ds_gitlab/OPM/db-utils.git "$DB_UTILS_DIR" '
              

##############################################################################
# dag
dag_interval = timedelta(hours=1)
dag = DAG(dag_id='covid-19-data-R',
          catchup=False,
          default_args=default_args,
          schedule_interval=dag_interval,
          concurrency=1)

##############################################################################
# Pull public data and push to minio
public_data_to_minio_operator = KubernetesPodOperator(
    cmds=["bash", "-cx"],
    arguments=["""{} &&
    cd "$CODE_REPO" &&
    Rscript public_data_to_minio.R""".format(startup_cmd)],
    name="fetcher-public-data-to-minio-{}".format(uuid.uuid4()),
    task_id='public_data_to_minio_operator',
    dag=dag,
    execution_timeout=timedelta(minutes=1200),
    **k8s_r_run_args
)

# Pull private data and push to minio
private_data_to_minio_operator = KubernetesPodOperator(
    cmds=["bash", "-cx"],
    arguments=["""{} &&
    cd "$CODE_REPO" &&
    Rscript private_data_to_minio.R""".format(startup_cmd)],
    name="fetcher-private-data-to-minio-{}".format(uuid.uuid4()),
    task_id='private_data_to_minio_operator',
    dag=dag,
    execution_timeout=timedelta(minutes=1200),
    **k8s_r_run_args
)

# Pull billing raw data and push to minio
billing_data_to_minio_operator = KubernetesPodOperator(
    cmds=["bash", "-cx"],
    arguments=["""{} &&
    cd "$CODE_REPO" &&
    python3 billing_finance_data_to_minio.py""".format(startup_cmd)],
    name="fetcher-billing-data-to-minio-{}".format(uuid.uuid4()),
    task_id='billing_data_to_minio_operator',
    dag=dag,
    execution_timeout=timedelta(minutes=1200),
    **k8s_py_run_args
)

# Munge billing raw data and push to minio
billing_data_munge_operator = KubernetesPodOperator(
    cmds=["bash", "-cx"],
    arguments=["""{} &&
    cd "$CODE_REPO" &&
    Rscript billing_finance_data_munge.R""".format(startup_cmd)],
    name="transform-billing-data-{}".format(uuid.uuid4()),
    task_id='billing_data_munge_operator',
    dag=dag,
    execution_timeout=timedelta(minutes=1200),
    **k8s_r_run_args
)


############################################################################
# Task ordering
billing_data_to_minio_operator >> billing_data_munge_operator

