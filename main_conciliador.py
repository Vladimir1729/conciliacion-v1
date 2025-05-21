from airflow.decorators import dag
from datetime import datetime
from slack_alerts import start_alert, success_alert, failure_alert
from comparador_tasks import comparar_referencias

CONFIG_PATH = ''

@dag(
    dag_id='comparar_referencias_dag',
    schedule_interval=None,
    start_date=datetime(2024, 8, 26),
    catchup=False,
    tags=['comparacion', 'bancos', 'pagaqui']
)
def comparar_dag():
    start = start_alert()
    result = comparar_referencias(config_path=CONFIG_PATH)
    success = success_alert(result)
    fail = failure_alert()

    start >> result
    result >> [success, fail]

dag = comparar_dag()
