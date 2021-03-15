from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret

from datetime import datetime, timedelta

DAG_STARTDATE = datetime(2020, 7, 31)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': DAG_STARTDATE,
    'email': ['gordon.inggs@capetown.gov.za'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

startup_cmd = (
    "mkdir $COVID_19_DATA_DIR && "
    "curl $COVID_19_DEPLOY_URL/$COVID_19_DEPLOY_FILE -o $COVID_19_DATA_DIR/$COVID_19_DEPLOY_FILE && "
    "cd $COVID_19_DATA_DIR && unzip $COVID_19_DEPLOY_FILE && "
    "pip3 install $DB_UTILS_LOCATION/$DB_UTILS_PKG"
)

dag_interval = "@daily"
dag_name = 'community-organisation-database'
dag = DAG(dag_name,
          start_date=DAG_STARTDATE,
          catchup=False,
          default_args=default_args,
          schedule_interval=dag_interval,
          concurrency=2)

# env variables for inside the k8s pod
k8s_run_env = {
    'SECRETS_FILE': '/secrets/secrets.json',
    'COVID_19_DEPLOY_FILE': 'covid-19-data.zip',
    'COVID_19_DEPLOY_URL': 'https://ds2.capetown.gov.za/covid-19-data-deploy',
    'COVID_19_DATA_DIR': '/covid-19-data',
    'DB_UTILS_LOCATION': 'https://ds2.capetown.gov.za/db-utils',
    'DB_UTILS_PKG': 'db_utils-0.3.7-py2.py3-none-any.whl'
}

# airflow-workers' secrets
secret_file = Secret('volume', '/secrets', 'airflow-workers-secret')

# arguments for the k8s operator
k8s_run_args = {
    "image": "cityofcapetown/datascience:python@sha256:c5a8ec97e35e603aca281343111193a26a929d821b84c6678fb381f9e7bd08d7",
    "namespace": 'airflow-workers',
    "is_delete_operator_pod": True,
    "get_logs": True,
    "in_cluster": True,
    "secrets": [secret_file],
    "env_vars": k8s_run_env,
    "image_pull_policy": "IfNotPresent",
     "startup_timeout_seconds": 60*30,
}


def covid_19_data_task(task_name, task_kwargs={}):
    """Factory for k8sPodOperator"""
    name = f"{dag_name}-{task_name}"
    run_args = {**k8s_run_args.copy(), **task_kwargs}
    run_cmd = "bash -c '{} && \"$COVID_19_DATA_DIR\"/bin/{}.sh'".format(startup_cmd, task_name)

    operator = KubernetesPodOperator(
        cmds=["bash", "-cx"],
        arguments=[run_cmd],
        name=name,
        task_id=name,
        dag=dag,
        execution_timeout=timedelta(hours=1),
        **run_args
    )

    return operator


# Defining tasks
COD_FETCH_TASK = 'cod-data-fetch'
cod_data_fetch_operator = covid_19_data_task(COD_FETCH_TASK)

COD_MUNGE_TASK = 'cod-data-munge'
cod_data_munge_operator = covid_19_data_task(COD_MUNGE_TASK)

# Dependencies
cod_data_fetch_operator >> cod_data_munge_operator
