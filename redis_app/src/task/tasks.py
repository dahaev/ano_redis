# Create your tasks here
from celery import shared_task
import psycopg2
from task.ano_task_download.main import download_data, start_time

host = '172.26.0.1'
user = 'postgres'
password = 'shisik'
name = 'redis_ano'

connection = psycopg2.connect(
    host=host,
    user=user,
    password=password,
    database=name
)
connection.autocommit = True

@shared_task
def update_data_base():
    with connection.cursor() as cursor:
        # cursor.execute(f"DELETE FROM ano_tasks WHERE upload_date = '{start_time}'")
        try:
            print("Table already deleted")
            cursor.execute('CREATE TABLE ano_tasks('
                           'id serial PRIMARY KEY,'
                           'project_id VARCHAR(50),'
                           'project_name VARCHAR(250),'
                           'task_code VARCHAR(50),'
                           'task_name VARCHAR(250),'
                           'task_date_start DATE,'
                           'task_date_end DATE,'
                           'task_done_percent FLOAT,'
                           'task_base_start DATE,'
                           'task_base_end DATE,'
                           'task_status VARCHAR(50),'
                           'task_active VARCHAR(50),'
                           'reaper_task VARCHAR(50),'
                           'reaper_3Q2022_plan DATE,'
                           'reaper_4Q2022_plan DATE,'
                           'reaper_1Q2023_plan DATE,'
                           'reaper_2Q2023_plan DATE,'
                           'reaper_3Q2023_plan DATE,'
                           'upload_date DATE'
                           ')')
        except:
            print('Удалены сведения из таблицы')

    with connection.cursor() as cursor:

        sql = """INSERT INTO ano_tasks(
                       project_id,
                       project_name,
                       task_code,
                       task_name,
                       task_date_start,
                       task_date_end,
                       task_done_percent,
                       task_base_end,
                       task_base_start,
                       task_status,
                       task_active,
                       reaper_task,
                       reaper_3Q2022_plan,
                       reaper_4Q2022_plan,
                       reaper_1Q2023_plan,
                       reaper_2Q2023_plan,
                       reaper_3Q2023_plan,
                       upload_date
                       ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                       """

        cursor.executemany(sql, download_data())
