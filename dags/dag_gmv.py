"""
DAG — GMV Purchase Snapshot (D-1)
===================================
Orquestra o pipeline ETL de GMV diário por subsidiária.

Gatilhos:
    As três tabelas de origem são gatilhos independentes para atualização
    da tabela final. A DAG monitora as pastas S3 de cada tabela via
    S3KeySensor. Quando qualquer uma delas recebe um arquivo novo referente
    ao dia anterior (D-1), o pipeline é disparado.

    purchase             ─┐
    product_item         ─┼─→ S3KeySensor (trigger_rule=ONE_SUCCESS) → EMR → MSCK
    purchase_extra_info  ─┘

Fluxo:
    1. Três sensores monitoram as pastas S3 de cada tabela (em paralelo).
    2. Assim que qualquer sensor detecta arquivo novo (ONE_SUCCESS), o job
       PySpark é submetido ao EMR.
    3. Após conclusão do job, MSCK REPAIR TABLE sincroniza as novas
       partições no Glue Data Catalog.
    4. Em caso de falha, alertas são enviados via SNS e e-mail em paralelo.

Configuração:
    - Substituir os valores de S3_BUCKET, EMR_CLUSTER_ID e SNS_TOPIC_ARN
      pelas referências reais do ambiente.
    - Configurar destinatários em Airflow Variable `gmv_alert_emails`
      (separados por vírgula). Ex: "eng@company.com,oncall@company.com"
    - Configurar SMTP no Airflow (airflow.cfg ou variável de ambiente
      AIRFLOW__SMTP__SMTP_HOST) para envio de e-mail.
    - O script gmv_etl.py deve estar disponível em s3://<bucket>/scripts/.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.trigger_rule import TriggerRule

# ================================================================
# CONFIGURAÇÃO
# ================================================================

S3_BUCKET      = Variable.get("gmv_s3_bucket",      default_var="your-bucket")
EMR_CLUSTER_ID = Variable.get("gmv_emr_cluster_id", default_var="j-XXXXXXXXXXXX")
SNS_TOPIC_ARN  = Variable.get("gmv_sns_topic_arn",  default_var="arn:aws:sns:us-east-1:000000000000:gmv-alerts")

# Lista de destinatários para alertas de falha
# Configurar em Airflow Variables ou substituir diretamente
ALERT_EMAILS = Variable.get(
    "gmv_alert_emails",
    default_var="data-team@company.com",
    deserialize_json=False,
).split(",")

# Prefixo D-1 no formato de partição do S3: dt=YYYY-MM-DD
D1_PREFIX = "dt={{ (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"

DEFAULT_ARGS = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,   # controlado manualmente via EmailOperator abaixo
    "email_on_retry": False,
}

# ================================================================
# DAG
# ================================================================

with DAG(
    dag_id="gmv_purchase_snapshot",
    description="Pipeline ETL de GMV diário por subsidiária — snapshot histórico D-1",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2023, 1, 1),
    schedule_interval="0 6 * * *",   # executa às 06h00 UTC todos os dias
    catchup=False,
    max_active_runs=1,
    tags=["gmv", "etl", "data-engineering"],
) as dag:

    # ================================================================
    # SENSORES — monitoram as três tabelas de origem em paralelo
    # ----------------------------------------------------------------
    # Cada sensor aguarda um arquivo novo na pasta S3 correspondente
    # ao dia anterior (D-1). O pipeline é disparado assim que qualquer
    # um dos três detectar dados novos (trigger_rule=ONE_SUCCESS nos
    # steps seguintes).
    #
    # poke_interval: verifica a cada 5 minutos
    # timeout: aguarda até 4 horas antes de falhar
    # mode="reschedule": libera o worker entre verificações
    # ================================================================

    sensor_purchase = S3KeySensor(
        task_id="sensor_purchase",
        bucket_name=S3_BUCKET,
        bucket_key=f"events/purchase/{D1_PREFIX}/",
        wildcard_match=True,
        poke_interval=300,
        timeout=14400,
        mode="reschedule",
        soft_fail=True,   # não bloqueia o pipeline se esta tabela não tiver evento
    )

    sensor_product_item = S3KeySensor(
        task_id="sensor_product_item",
        bucket_name=S3_BUCKET,
        bucket_key=f"events/product_item/{D1_PREFIX}/",
        wildcard_match=True,
        poke_interval=300,
        timeout=14400,
        mode="reschedule",
        soft_fail=True,
    )

    sensor_extra_info = S3KeySensor(
        task_id="sensor_extra_info",
        bucket_name=S3_BUCKET,
        bucket_key=f"events/purchase_extra_info/{D1_PREFIX}/",
        wildcard_match=True,
        poke_interval=300,
        timeout=14400,
        mode="reschedule",
        soft_fail=True,
    )

    # ================================================================
    # JOB EMR — submete o PySpark ao cluster
    # ----------------------------------------------------------------
    # trigger_rule=ONE_SUCCESS: executa assim que qualquer sensor
    # detectar evento novo, sem aguardar os demais.
    # Isso implementa o requisito: "todas as tabelas são gatilhos".
    # ================================================================

    submit_emr_step = EmrAddStepsOperator(
        task_id="submit_gmv_etl",
        job_flow_id=EMR_CLUSTER_ID,
        steps=[
            {
                "Name": "gmv_purchase_snapshot_etl",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode", "cluster",
                        "--conf", "spark.sql.parquet.compression.codec=snappy",
                        f"s3://{S3_BUCKET}/scripts/gmv_etl.py",
                    ],
                },
            }
        ],
        trigger_rule=TriggerRule.ONE_SUCCESS,  # gatilho: qualquer tabela com evento novo
    )

    # ================================================================
    # SENSOR EMR — aguarda o job finalizar
    # ================================================================

    wait_emr_step = EmrStepSensor(
        task_id="wait_gmv_etl",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull('submit_gmv_etl', key='return_value')[0] }}",
        poke_interval=60,
        timeout=7200,
        mode="reschedule",
    )

    # ================================================================
    # MSCK REPAIR TABLE — sincroniza partições no Glue Data Catalog
    # ----------------------------------------------------------------
    # Após o PySpark gravar novas partições no S3, o Athena não as
    # enxerga automaticamente. Este step garante que as partições
    # fiquem disponíveis para consulta imediatamente após o pipeline.
    # ================================================================

    repair_table = EmrAddStepsOperator(
        task_id="repair_glue_partitions",
        job_flow_id=EMR_CLUSTER_ID,
        steps=[
            {
                "Name": "msck_repair_gmv_purchase_snapshot",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "hive", "-e",
                        "MSCK REPAIR TABLE gmv_purchase_snapshot;",
                    ],
                },
            }
        ],
    )

    wait_repair = EmrStepSensor(
        task_id="wait_repair_partitions",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull('repair_glue_partitions', key='return_value')[0] }}",
        poke_interval=30,
        timeout=600,
        mode="reschedule",
    )

    # ================================================================
    # ALERTA DE FALHA — notifica via SNS + e-mail
    # ----------------------------------------------------------------
    # Dois canais de notificação em paralelo:
    #   1. SNS: para integração com outros sistemas (PagerDuty, Slack, etc.)
    #   2. E-mail: direto para os destinatários configurados em ALERT_EMAILS
    #
    # Ambos disparam com trigger_rule=ONE_FAILED — qualquer task
    # com falha aciona os dois alertas simultaneamente.
    # ================================================================

    def notify_sns(context):
        import boto3
        sns     = boto3.client("sns")
        dag_id  = context["dag"].dag_id
        task_id = context["task_instance"].task_id
        ds      = context["ds"]
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"[FALHA] DAG {dag_id} — {ds}",
            Message=(
                f"DAG: {dag_id}\n"
                f"Task: {task_id}\n"
                f"Data de execução: {ds}\n"
                f"Log: {context['task_instance'].log_url}"
            ),
        )

    alert_sns = PythonOperator(
        task_id="alert_sns",
        python_callable=notify_sns,
        trigger_rule=TriggerRule.ONE_FAILED,
        provide_context=True,
    )

    alert_email = EmailOperator(
        task_id="alert_email",
        to=ALERT_EMAILS,
        subject="[FALHA] DAG gmv_purchase_snapshot — {{ ds }}",
        html_content="""
            <h3 style="color:#c0392b;">Falha no pipeline GMV</h3>
            <table style="border-collapse:collapse;font-family:sans-serif;font-size:14px;">
                <tr><td style="padding:4px 12px 4px 0;color:#666;">DAG</td>
                    <td><strong>{{ dag.dag_id }}</strong></td></tr>
                <tr><td style="padding:4px 12px 4px 0;color:#666;">Data</td>
                    <td>{{ ds }}</td></tr>
                <tr><td style="padding:4px 12px 4px 0;color:#666;">Execução</td>
                    <td>{{ execution_date }}</td></tr>
            </table>
            <p style="margin-top:16px;">
                <a href="{{ task_instance.log_url }}"
                   style="background:#c0392b;color:#fff;padding:8px 16px;
                          border-radius:4px;text-decoration:none;">
                    Ver log
                </a>
            </p>
            <p style="font-size:12px;color:#999;margin-top:16px;">
                Verifique o Airflow para detalhes e realize o reprocessamento se necessário.
            </p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # ================================================================
    # DEPENDÊNCIAS
    # ================================================================

    # Sensores em paralelo → job EMR → repair
    [sensor_purchase, sensor_product_item, sensor_extra_info] >> submit_emr_step
    submit_emr_step >> wait_emr_step >> repair_table >> wait_repair

    # Qualquer falha no pipeline dispara SNS + e-mail em paralelo
    [submit_emr_step, wait_emr_step, repair_table] >> alert_sns
    [submit_emr_step, wait_emr_step, repair_table] >> alert_email