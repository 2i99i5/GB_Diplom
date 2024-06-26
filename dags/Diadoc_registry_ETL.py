import datetime
import pendulum
import os
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator

# блок констант
DAGS_FLDR = '/home/ruslan/airflow/dags/'
DWNLD_FLDR = '/home/ruslan/Загрузки'
LOAD_TIME = 60  # Время на формирование реестра для сервера Диадок
YEAR = 2018  # данные за этот год будут загружены
INTERVAL = 'Год'  # При выборе "Интервал" будут загружены все документы, но время нужно кратно увеличивать
TABLE_STRUCTURE = """(
    inn TEXT, 
    kpp TEXT, 
    organization_name TEXT, 
    filename TEXT, 
    document_number TEXT, 
    document_date TEXT, 
    total TEXT, 
    currency TEXT, 
    vat TEXT, 
    vat_10 TEXT, 
    vat_18 TEXT, 
    vat_20 TEXT, 
    delivery_date TEXT, 
    document_status TEXT, 
    tarification_date TEXT, 
    status_change_date TEXT, 
    subdivision TEXT, 
    responsible_person TEXT, 
    comment TEXT, 
    link TEXT PRIMARY KEY, 
    deleted TEXT,
    docs_type VARCHAR(6)
)"""


@dag(
    dag_id="diadoc-registry",
    schedule_interval="0 2 * * 7",  # запуск каждое воскресенье в 02:00
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessDiadocRegistry():
    # скачивание реестра Входящих
    selenium_download_inbox = BashOperator(
        task_id='selenium_download_inbox',
        bash_command=f'python {DAGS_FLDR}selenium/Diadoc_download_registry.py '
                     f'--docs_type=Inbox '
                     f'--load_time={LOAD_TIME} '
                     f'--date_range_mode={INTERVAL} '
                     f'--year={YEAR}'
    )
    # преобразование CSV входящих
    csv_find_encrypt_inbox = BashOperator(
        task_id="csv_find_encrypt_inbox",
        bash_command=f'python {DAGS_FLDR}python/CSV_find_encrypt_delete.py '
                     f'--downloads_path={DWNLD_FLDR} '
                     f'--output_file_path={DAGS_FLDR}files/output.csv '
                     f'--docs_type=Inbox'
    )
    # скачивание реестра Исходящих
    selenium_download_outbox = BashOperator(
        task_id='selenium_download_outbox',
        bash_command=f'python {DAGS_FLDR}selenium/Diadoc_download_registry.py '
                     f'--docs_type=Outbox '
                     f'--load_time={LOAD_TIME} '
                     f'--date_range_mode={INTERVAL} '
                     f'--year={YEAR}'
    )
    # преобразование CSV исходящих
    csv_find_encrypt_outbox = BashOperator(
        task_id="csv_find_encrypt_outbox",
        bash_command=f'python {DAGS_FLDR}python/CSV_find_encrypt_delete.py '
                     f'--downloads_path={DWNLD_FLDR} '
                     f'--output_file_path={DAGS_FLDR}files/output.csv '
                     f'--docs_type=Outbox'
    )
    # создание таблицы хранения данных
    create_diadoc_registry_table = PostgresOperator(
        task_id="create_registry_table",
        postgres_conn_id="pg_conn",
        sql=f'CREATE TABLE IF NOT EXISTS diadoc_registry {TABLE_STRUCTURE};',
    )
    # создание временной таблицы
    create_diadoc_temp_table = PostgresOperator(
        task_id="create_registry_temp_table",
        postgres_conn_id="pg_conn",
        sql=f"""DROP TABLE IF EXISTS diadoc_temp;
            CREATE TABLE diadoc_temp {TABLE_STRUCTURE};""",
    )

    @task
    def delete_loaded_csv():
        try:
            data_path = f"{DAGS_FLDR}files/output.csv"  # путь к CSV с данными
            # Удаление файла из папки Загрузки
            os.remove(data_path)
            print("Файл успешно удален из папки.")
            return 0
        except Exception as e:
            print("Файл не удалось удалить папки.")
            return 1

    # загрузка данных во временную таблицу
    @task
    def get_data():
        data_path = f"{DAGS_FLDR}files/output.csv"  # путь к CSV с данными
        os.makedirs(os.path.dirname(data_path), exist_ok=True)

        postgres_hook = PostgresHook(postgres_conn_id="pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open(data_path, "r") as file:
            cur.copy_expert(
                "COPY diadoc_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        conn.commit()

    # обновление данных в постоянной таблице
    @task
    def merge_data():
        query = """
            INSERT INTO diadoc_registry
            SELECT *
            FROM (
                SELECT DISTINCT *
                FROM diadoc_temp
            )
            ON CONFLICT (link) DO UPDATE
            SET document_status = excluded.document_status, 
                tarification_date = excluded.tarification_date, 
                status_change_date = excluded.status_change_date, 
                subdivision = excluded.subdivision, 
                link = excluded.link, 
                deleted = excluded.deleted,
                docs_type =  excluded.docs_type;
            TRUNCATE TABLE diadoc_temp;
        """
        try:
            postgres_hook = PostgresHook(postgres_conn_id="pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            return 0
        except Exception as e:
            return 1

    (selenium_download_inbox >> csv_find_encrypt_inbox
     >> [create_diadoc_registry_table, create_diadoc_temp_table]
     >> get_data() >> merge_data() >> delete_loaded_csv()
     >> selenium_download_outbox >> csv_find_encrypt_outbox
     >> get_data() >> merge_data() >> delete_loaded_csv())


dag = ProcessDiadocRegistry()
