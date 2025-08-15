
import datetime
from datetime import datetime
import os
from airflow import models
from kubernetes.client import models as k8s
# from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

# Instantiating a DAG object

with models.DAG(
    dag_id="ceba_dag",
    description="ceba",
    schedule_interval='0 13 * * *',
    start_date=datetime(2025, 3, 24),
) as dag:

  task_1 = KubernetesPodOperator(
    task_id="task_1",
    name="task_ceba",
    startup_timeout_seconds=600,
    namespace=os.environ["K8_NAMESPACE"],
    image="us-central1-docker.pkg.dev/cida-tenant-deploy-vx9m/cidat-10040/ceba_alexye:0.0.1",
    cmds=["python"],
    arguments=["ceba_automation_v1.py"],
    get_logs=True,
    image_pull_policy='Always',
    config_file="/home/airflow/composer_kube_config",
    kubernetes_conn_id='kubernetes_default',
  )

task_1
