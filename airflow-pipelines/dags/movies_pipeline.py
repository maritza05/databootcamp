import tempfile
from datetime import timedelta, datetime

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import (
    PostgresToGCSOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryExecuteQueryOperator,
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitPySparkJobOperator,
    DataprocDeleteClusterOperator,
)


CSV_FILE = Variable.get("USER_PURCHASE_CSV_FILE")
PROJECT_ID = Variable.get("PROJECT_ID")
USER_PURCHASE_TABLE_NAME = Variable.get("USER_PURCHASE_TABLE")
SLACK_CHANNEL = Variable.get("SLACK_CHANNEL")


def _send_success_notification(context):
    return _send_slack_notification(":green_circle: Task Succeded!", context)


def _send_fail_notification(context):
    return _send_slack_notification(":red_circle: Task Failed", context)


def _send_slack_notification(message, context):
    SlackWebhookOperator(
        task_id="send_notification",
        http_conn_id="slack_conn",
        message=message,
        channel=SLACK_CHANNEL,
    ).execute(context=context)


def _get_dim_query(id_column, dim_column):
    table_id = "{{var.value.LOGS_EXTERNAL_TABLE_NAME}}"
    dataset_name = "{{var.value.WAREHOUSE_DATASET}}"
    return f"""
    SELECT ROW_NUMBER() OVER(ORDER BY t.{dim_column}) AS {id_column},
    t.{dim_column} as {dim_column} FROM
    (SELECT DISTINCT {dim_column} FROM `{PROJECT_ID}.{dataset_name}.{table_id}`) t
    """


def _get_dim_date_query():
    table_id = "{{var.value.LOGS_EXTERNAL_TABLE_NAME}}"
    dataset_name = "{{var.value.WAREHOUSE_DATASET}}"
    return f"""
    WITH log_dates AS (
        SELECT DISTINCT(DATE_FROM_UNIX_DATE(log_date)) as log_date
        FROM `{PROJECT_ID}.{dataset_name}.{table_id}`
    )
    SELECT ROW_NUMBER() OVER(ORDER BY ld.log_date) AS id_dim_date,
    ld.log_date,
    EXTRACT(DAY FROM ld.log_date) AS day,
    EXTRACT(MONTH FROM ld.log_date) AS month,
    EXTRACT(YEAR FROM ld.log_date) AS year,
    {dataset_name}.getSeason(EXTRACT(MONTH FROM ld.log_date))
    AS season FROM log_dates AS ld;"""


default_args = {
    "owner": "maritza",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "maritzaesparza05@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "on_failure_callback": _send_fail_notification,
    "on_success_callback": _send_success_notification,
}


def _write_data_on_db(csv_file):
    df = pd.read_csv(csv_file)
    df = df.dropna(subset=["CustomerID"]).copy()
    df["CustomerID"] = df["CustomerID"].apply(int)

    with tempfile.TemporaryDirectory() as tempdir:
        df.to_csv(f"{tempdir}/temp.csv", index=False, header=False)

        sql = f"""
        COPY {USER_PURCHASE_TABLE_NAME}
        FROM STDIN
        DELIMITER ',' CSV;
        """

        postgres_hook = PostgresHook(postgres_con_id="postgres_default")
        postgres_hook.copy_expert(sql=sql, filename=f"{tempdir}/temp.csv")


with DAG(
    "user_purchase_pipeline",
    start_date=datetime(2022, 1, 26),
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:

    create_user_purchase_table = PostgresOperator(
        task_id="create_user_purchase_table",
        postgres_conn_id="postgres_default",
        sql=f"""
            CREATE TABLE IF NOT EXISTS {USER_PURCHASE_TABLE_NAME} (
            invoice_number VARCHAR,
            stock_code VARCHAR,
            detail VARCHAR,
            quantity INT,
            invoice_date TIMESTAMP,
            unit_price REAL,
            customer_id VARCHAR,
            country VARCHAR);
          """,
    )

    write_user_purchase_data_on_db = PythonOperator(
        task_id="write_data_on_db",
        python_callable=_write_data_on_db,
        op_kwargs={"csv_file": CSV_FILE},
    )

    export_user_purchase_db_to_csv = PostgresToGCSOperator(
        task_id="export_user_purchase_db_to_csv",
        postgres_conn_id="postgres_default",
        google_cloud_storage_conn_id="google_default",
        sql=f"select customer_id, quantity, unit_price from {USER_PURCHASE_TABLE_NAME};",
        bucket="{{var.value.STAGING_BUCKET}}",
        filename="{{var.value.CLEAN_USER_PURCHASE_FILE}}",
        export_format="CSV",
        gzip=False,
        use_server_side_cursor=True,
    )

    create_user_purchase_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_external_user_purchase_table",
        bucket="{{var.value.STAGING_BUCKET}}",
        table_resource={
            "tableReference": {
                "projectId": "{{var.value.PROJECT_ID}}",
                "datasetId": "{{var.value.WAREHOUSE_DATASET}}",
                "tableId": "{{var.value.USER_PURCHASE_EXTERNAL_TABLE_NAME}}",
            },
            "schema": {
                "fields": [
                    {"name": "customer_id", "type": "STRING"},
                    {"name": "quantity", "type": "INT64"},
                    {"name": "unit_price", "type": "FLOAT64"},
                ]
            },
            "externalDataConfiguration": {
                "sourceFormat": "CSV",
                "compression": "NONE",
                "csvOptions": {"skipLeadingRows": 1},
                "sourceUris": ["{{var.value.USER_PURCHASE_EXTERNAL_TABLE_URI}}"],
            },
        },
    )

    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_config={
            "temp_bucket": "{{var.value.DATAPROC_TEMP_BUCKET}}",
            "software_config": {"image_version": "2.0"},
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-standard-2",
                "disk_config": {
                    "boot_disk_type": "pd-standard",
                    "boot_disk_size_gb": 500,
                },
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "n1-standard-2",
                "disk_config": {
                    "boot_disk_type": "pd-standard",
                    "boot_disk_size_gb": 500,
                },
            },
        },
        region="{{var.value.DATAPROC_REGION}}",
        cluster_name="{{var.value.DATAPROC_CLUSTER_NAME}}",
        gcp_conn_id="google_default",
    )

    submit_movies_reviews_job = DataprocSubmitPySparkJobOperator(
        task_id="submit_movies_reviews_job",
        main="{{var.value.MOVIES_REVIEWS_SCRIPT}}",
        gcp_conn_id="google_default",
        cluster_name="{{var.value.DATAPROC_CLUSTER_NAME}}",
        job_name="movie_reviews_job",
        dataproc_jars=[
            "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.1.1/spark-avro_2.12-3.1.1.jar"
        ],
        arguments=[
            "--input_file",
            "{{var.value.MOVIES_REVIEWS_INPUT}}",
            "--output_path",
            "{{var.value.MOVIES_REVIEWS_OUTPUT}}",
        ],
        region="{{var.value.DATAPROC_REGION}}",
        project_id=PROJECT_ID,
    )

    submit_logs_reviews_job = DataprocSubmitPySparkJobOperator(
        task_id="submit_logs_reviews_job",
        gcp_conn_id="google_default",
        main="{{var.value.LOGS_REVIEWS_SCRIPT}}",
        cluster_name="{{var.value.DATAPROC_CLUSTER_NAME}}",
        job_name="logs_reviews_job",
        dataproc_jars=[
            "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.1.1/spark-avro_2.12-3.1.1.jar"
        ],
        arguments=[
            "--input_file",
            "{{var.value.LOGS_REVIEWS_INPUT}}",
            "--output_path",
            "{{var.value.LOGS_REVIEWS_OUTPUT}}",
        ],
        region="{{var.value.DATAPROC_REGION}}",
        project_id=PROJECT_ID,
    )

    delete_cluster_task = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name="{{var.value.DATAPROC_CLUSTER_NAME}}",
        region="{{var.value.DATAPROC_REGION}}",
        gcp_conn_id="google_default",
    )

    create_movies_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_external_movies_reviews_table",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": "{{var.value.WAREHOUSE_DATASET}}",
                "tableId": "movies_reviews",
            },
            "externalDataConfiguration": {
                "sourceFormat": "AVRO",
                "compression": "NONE",
                "sourceUris": ["{{var.value.MOVIES_EXTERNAL_TABLE_URI}}"],
            },
        },
    )

    create_logs_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_external_log_reviews_table",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": "{{var.value.WAREHOUSE_DATASET}}",
                "tableId": "logs_reviews",
            },
            "externalDataConfiguration": {
                "sourceFormat": "AVRO",
                "compression": "NONE",
                "sourceUris": ["{{var.value.LOGS_EXTERNAL_TABLE_URI}}"],
            },
        },
    )

    create_dim_location_table = BigQueryCreateEmptyTableOperator(
        task_id="create_dim_location_table",
        dataset_id="{{var.value.WAREHOUSE_DATASET}}",
        table_id="dim_location",
        view={
            "query": _get_dim_query("id_dim_location", "location"),
            "useLegacySql": False,
        },
    )

    create_dim_os_table = BigQueryCreateEmptyTableOperator(
        task_id="create_dim_os_table",
        dataset_id="{{var.value.WAREHOUSE_DATASET}}",
        table_id="dim_os",
        view={
            "query": _get_dim_query("id_dim_os", "os"),
            "useLegacySql": False,
        },
    )

    create_dim_device_table = BigQueryCreateEmptyTableOperator(
        task_id="create_dim_device_table",
        dataset_id="{{var.value.WAREHOUSE_DATASET}}",
        table_id="dim_devices",
        view={
            "query": _get_dim_query("id_dim_device", "device"),
            "useLegacySql": False,
        },
    )

    create_dim_date_table = BigQueryCreateEmptyTableOperator(
        task_id="create_dim_date_table",
        dataset_id="{{var.value.WAREHOUSE_DATASET}}",
        table_id="dim_date",
        view={
            "query": _get_dim_date_query(),
            "useLegacySql": False,
        },
    )

    create_facts_table = BigQueryExecuteQueryOperator(
        task_id="create_facts_table",
        sql="facts_table.sql",
        params={
            "project_id": PROJECT_ID,
            "dataset_name": Variable.get("WAREHOUSE_DATASET"),
            "logs_external_table": Variable.get("LOGS_EXTERNAL_TABLE_NAME"),
            "movies_external_table": Variable.get("MOVIES_EXTERNAL_TABLE_NAME"),
            "user_purchase_table": Variable.get("USER_PURCHASE_EXTERNAL_TABLE_NAME"),
        },
        destination_dataset_table="{{var.value.WAREHOUSE_DATASET}}.facts_movie_analytics",
        allow_large_results=True,
        use_legacy_sql=False,
    )

    (
        create_user_purchase_table
        >> write_user_purchase_data_on_db
        >> export_user_purchase_db_to_csv
        >> create_user_purchase_external_table
        >> create_facts_table
    )

    (create_dataproc_cluster >> [submit_movies_reviews_job, submit_logs_reviews_job])

    (submit_movies_reviews_job >> create_movies_external_table >> create_facts_table)

    (
        submit_logs_reviews_job
        >> create_logs_external_table
        >> [
            create_dim_location_table,
            create_dim_os_table,
            create_dim_device_table,
            create_dim_date_table,
        ]
        >> create_facts_table
    )

    create_facts_table >> delete_cluster_task
