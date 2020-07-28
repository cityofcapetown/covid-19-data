from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret

from datetime import datetime, timedelta

DAG_STARTDATE = datetime(2020, 4, 24)
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

dag_interval = "@hourly"
dag = DAG('covid-19-wcgh-data',
          start_date=DAG_STARTDATE,
          catchup=False,
          default_args=default_args,
          schedule_interval=dag_interval,
          concurrency=2)

# env variables for inside the k8s pod
k8s_run_env = {
    'SECRETS_PATH': '/secrets/wcgh-secrets.json',
    'COVID_19_DEPLOY_FILE': 'covid-19-data.zip',
    'COVID_19_DEPLOY_URL': 'https://ds2.capetown.gov.za/covid-19-data-deploy',
    'COVID_19_DATA_DIR': '/covid-19-data',
    'DB_UTILS_LOCATION': 'https://ds2.capetown.gov.za/db-utils',
    'DB_UTILS_PKG': 'db_utils-0.3.2-py2.py3-none-any.whl'
}

# airflow-workers' secrets
secret_file = Secret('volume', '/secrets', 'wcgh-secret')

# arguments for the k8s operator
k8s_run_args = {
    "image": "cityofcapetown/datascience:python",
    "namespace": 'airflow-workers',
    "is_delete_operator_pod": True,
    "get_logs": True,
    "in_cluster": True,
    "secrets": [secret_file],
    "env_vars": k8s_run_env,
    "image_pull_policy": "Always",
    "startup_timeout_seconds": 60 * 30,
}


def covid_19_data_task(task_name, task_kwargs={}):
    """Factory for k8sPodOperator"""
    name = "covid-19-wcgh-data-{}".format(task_name)
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
WCGH_FETCH_TASK = 'wcgh-data-fetch'
wcgh_data_fetch_operator = covid_19_data_task(WCGH_FETCH_TASK)

WCGH_PUSH_TASK = 'wcgh-data-push'
wcgh_data_push_operator = covid_19_data_task(WCGH_PUSH_TASK)

WCGH_CKAN_PUSH_TASK = 'wcgh-ckan-data-push'
wcgh_ckan_data_push_operator = covid_19_data_task(WCGH_CKAN_PUSH_TASK)

SPV_COLLECT_TASK = 'spv-data-fetch'
spv_data_fetch_operator = covid_19_data_task(SPV_COLLECT_TASK)

SPV_LAG_TASK = 'spv-lag-munge'
spv_data_munge_operator = covid_19_data_task(SPV_LAG_TASK)

SPV_ADJUST_TASK = 'spv-adjust-munge'
spv_adjust_munge_operator = covid_19_data_task(SPV_LAG_TASK)

# Dependencies
wcgh_data_fetch_operator >> wcgh_data_push_operator >> wcgh_ckan_data_push_operator
wcgh_data_fetch_operator >> spv_data_fetch_operator >> spv_data_munge_operator >> spv_adjust_munge_operator