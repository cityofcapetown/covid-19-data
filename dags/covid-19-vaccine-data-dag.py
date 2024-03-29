from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret
from datetime import datetime, timedelta

DAG_STARTDATE = datetime(2021, 3, 5)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': DAG_STARTDATE,
    'email': ['colin.anthony2@capetown.gov.za', 'gordon.inggs@capetown.gov.za'],
    'email_on_failure': True,
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

dag_interval = timedelta(hours=1)
dag_name = "covid-19-vaccine-data"
dag = DAG(dag_name,
          start_date=DAG_STARTDATE,
          catchup=False,
          default_args=default_args,
          schedule_interval=dag_interval,
          concurrency=2)

# env variables for inside the k8s pod
k8s_run_env = {
    'SECRETS_PATH': '/secrets/secrets.json',
    'COVID_19_DEPLOY_FILE': 'covid-19-data.zip',
    'COVID_19_DEPLOY_URL': 'https://ds2.capetown.gov.za/covid-19-data-deploy',
    'COVID_19_DATA_DIR': '/covid-19-data',
    'DB_UTILS_LOCATION': 'https://ds2.capetown.gov.za/db-utils',
    'DB_UTILS_PKG': 'db_utils-0.4.0-py2.py3-none-any.whl',
}

# airflow-workers' secrets
secret_file = Secret('volume', '/secrets', 'vaccine-data-secret')

# arguments for the k8s operator
k8s_run_args = {
    "image": "cityofcapetown/datascience:python@sha256:491802742dabd1eb6c550d220b6d3f3e6ac4359b8ded3307416831583cbcdee9",
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
    name = f"{dag_name}-{task_name}"
    run_args = {**k8s_run_args.copy(), **task_kwargs}
    run_cmd = f"bash -c '{startup_cmd} && \"$COVID_19_DATA_DIR\"/bin/{task_name}.sh'"

    operator = KubernetesPodOperator(
        cmds=["bash", "-cx"],
        arguments=[run_cmd],
        name=name,
        task_id=name,
        dag=dag,
        execution_timeout=timedelta(hours=2),

        **run_args
    )

    return operator


# Defining tasks
VACCINE_FETCH_TASK = "vaccine-data-to-minio"
vaccine_data_fetch_operator = covid_19_data_task(VACCINE_FETCH_TASK)

VACCINE_STAFF_PUSH_TASK = "vaccine-staff-data-to-wcgh"
vaccine_staff_data_push_operator = covid_19_data_task(VACCINE_STAFF_PUSH_TASK)

VACCINE_SEQ_MUNGE = "vaccine-sequencing-munge"
vaccine_seq_munge_operator = covid_19_data_task(VACCINE_SEQ_MUNGE)

VACCINE_SEQ_AGG_TASK = "vaccine-sequencing-aggregation"
vaccine_seq_agg_operator = covid_19_data_task(VACCINE_SEQ_AGG_TASK)

VACCINE_ANN_HR_MUNGE_TASK = "vaccine-annotate-hr-munge"
vaccine_ann_hr_munge_operator = covid_19_data_task(VACCINE_ANN_HR_MUNGE_TASK)

VACCINE_ANN_NON_HR_MUNGE_TASK = "vaccine-annotate-non-hr-munge"
vaccine_ann_non_hr_munge_operator = covid_19_data_task(VACCINE_ANN_NON_HR_MUNGE_TASK)

VACCINE_REG_AGG_MUNGE_TASK = "vaccine-register-munge"
vaccine_agg_munge_operator = covid_19_data_task(VACCINE_REG_AGG_MUNGE_TASK)

VACCINE_REG_AGG_WILLING_MUNGE_TASK = "vaccine-register-willing-munge"
vaccine_agg_willing_munge_operator = covid_19_data_task(VACCINE_REG_AGG_WILLING_MUNGE_TASK)

VACCINE_TIME_SERIES_MUNGE_TASK = "vaccine-rollout-time-series"
vaccine_time_series_munge_operator = covid_19_data_task(VACCINE_TIME_SERIES_MUNGE_TASK)

VACCINE_TIME_SERIES_WILLING_MUNGE_TASK = "vaccine-rollout-willing-time-series"
vaccine_time_series_willing_munge_operator = covid_19_data_task(VACCINE_TIME_SERIES_WILLING_MUNGE_TASK)

# Dependencies
vaccine_data_fetch_operator >> (vaccine_seq_munge_operator, vaccine_ann_hr_munge_operator)
vaccine_seq_munge_operator >> vaccine_seq_agg_operator
vaccine_ann_hr_munge_operator >> vaccine_ann_non_hr_munge_operator >> vaccine_agg_munge_operator
vaccine_agg_munge_operator >> vaccine_time_series_munge_operator
vaccine_seq_agg_operator >> vaccine_ann_non_hr_munge_operator >> vaccine_agg_willing_munge_operator
vaccine_agg_willing_munge_operator >> vaccine_time_series_willing_munge_operator