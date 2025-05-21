from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta

def send_alert(message: str, task_id: str):
    return SlackWebhookOperator(
        task_id=task_id,
        slack_webhook_conn_id='slack_default',
        message=message,
        username='airflow',
        channel='#etl',
        retries=3,
        retry_delay=timedelta(minutes=5)
    )

@task()
def start_alert():
    return send_alert(
        ":rocket: El DAG *comparar_referencias* ha comenzado.",
        "start_alert"
    ).execute(context={})

@task()
def success_alert(result: dict):
    message = (
        ":white_check_mark: DAG *comparar_referencias* finalizado exitosamente.\n"
        f"• Coincidencias encontradas: *{result['coincidencias']}*\n"
        f"• Registros a favor: *{result['a_favor']}* (Suma MONTO: *{result['suma_monto']}*)\n"
        f"• Registros en contra: *{result['en_contra']}* (Suma NumAbono: *{result['suma_abono']}*)\n"
        f"• Archivo generado: {result['archivo_gcs']}"
    )
    return send_alert(message, "success_alert").execute(context={})

@task(trigger_rule=TriggerRule.ONE_FAILED)
def failure_alert():
    return send_alert(
        ":red_circle: El DAG *comparar_referencias* ha fallado.",
        "failure_alert"
    ).execute(context={})
