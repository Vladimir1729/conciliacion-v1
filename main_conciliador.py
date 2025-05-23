import os
from airflow.decorators import dag
from datetime import datetime
from slack_alerts import start_alert, success_alert, failure_alert
from dag_comparador import comparar_referencias


#Ruta del directorio donde están los JSON
#CONFIG_PATH = ''
CONFIG_DIR = os.path.join(os.path.dirname(__file__), 'configs')


@dag(
    dag_id='comparar_referencias_dag',
    schedule_interval=None,
    start_date=datetime(2024, 8, 26),
    catchup=False,
    tags=['comparacion', 'bancos', 'pagaqui']
)
def comparar_dag():
    start = start_alert()
    all_results = []


    #iteramos sobre todos los archivos (.json) del directorio
    for file in os.listdir(CONFIG_DIR):
        if file.endswith('.json'):
            config_path = os.path.join(CONFIG_DIR, file)
            result = comparar_referencias.override(task_id = f"comparar_{file.replace('.json', '')}")(config_path = config_path)
            start >> result
            all_results.append(result)

            #Agregamos alertas individuales por cada comparación si deseas
            result >> success_alert.override(task_id = f"success_alert_{file.replace('.json', '')}")(result)
            result >> failure_alert.override(task_id = f"failure_alert_{file.replace('.json', '')}")()


    '''
    result = comparar_referencias(config_path=CONFIG_PATH)
    success = success_alert(result)
    fail = failure_alert()

    start >> result
    result >> [success, fail]
    '''

dag = comparar_dag()
