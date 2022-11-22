import requests
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, date

test_projects = (
    '2ed5b9ec-cef9-eb11-811a-00155d7f2547',
    '24d80de2-bca5-ec11-811f-00155d7f9c7d',
    '38911f31-5075-ec11-b38c-c8d9d22ecdc4',
    '0000cf75-fb12-4ffc-a404-aec4f3258a9c'
)

start_time = datetime.now().date()


def datetime_to_date(value: str):
    if value:
        value_date = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S").date()
        return value_date
    return None


class UploadedDate:
    start_time = datetime.now().date()

class Query:
    project_id = 'ИДПроекта'
    project_name = 'ИмяПроекта'
    task_date_end = 'ДатаОкончанияЗадачи'
    task_active = 'ЗадачаЯвляетсяАктивной'
    task_name = 'НазваниеЗадачи'
    task_done_percent = 'ПроцентЗавершенияЗадачи'
    task_date_start = 'ДатаНачалаЗадачи'
    task_code = 'Кодзадачи'
    task_status = 'Статусзадачи'
    task_base_end = 'ДатаокончанияБП0'
    task_base_start = 'ДатаначалаБП0'
    reaper_3Q2022_plan = 'РеперныезадачиПланIIIквартал2022'
    reaper_4Q2022_plan = 'РеперныезадачиПланIVквартал2022'
    reaper_1Q2023_plan = 'РеперныезадачиПланIквартал2023'
    reaper_2Q2023_plan = 'РеперныезадачиПланIIквартал2023'
    reaper_3Q2023_plan = 'РеперныезадачиПланIIIквартал2023'
    reaper_task = 'Реперныезадачи'


class TasksSerialize(BaseModel):
    project_id: Optional[str]
    project_name: Optional[str]
    task_date_end: Optional[date]
    task_active: Optional[str]
    task_name: Optional[str]
    task_done_percent: Optional[float]
    task_date_start: Optional[date]
    task_code: Optional[str]
    task_status: Optional[str]
    task_base_end: Optional[date]
    task_base_start: Optional[date]
    reaper_3Q2022_plan: Optional[date]
    reaper_4Q2022_plan: Optional[date]
    reaper_1Q2023_plan: Optional[date]
    reaper_2Q2023_plan: Optional[date]
    reaper_3Q2023_plan: Optional[date]
    reaper_task: Optional[str]


class RequestAno:

    def __init__(self):
        self.url = 'https://spanorsi.lancloud.ru/pwa/_api/ProjectData/'
        self.table = 'Задачи'
        self.auth = ('conteq_1@ano-rsi.ru', 'BesB8288')
        self.params = {'$format': 'json'}
        self.__add_params = dict()

    @property
    def add_params(self):
        return self.__add_params

    @add_params.setter
    def add_params(self, new: dict):
        if not isinstance(new, dict):
            raise TypeError('Wrong type of query')
        self.__add_params = new
        return self.__add_params

    def query_params(self):
        query = self.params | self.__add_params
        return query

    def ConnectAno(self, urlLink, table=''):
        response = requests.get(url=self.url + table + urlLink, auth=self.auth, params=self.query_params())
        if response.status_code != 200:
            raise ConnectionError('No connection to SERVER ANO')
        return response

    def DataResponse(self):
        ANOResponse = []
        next_url = ''
        while True:
            print(f'Uploaded Tasks: {len(ANOResponse)}')
            response = self.ConnectAno(self.table, next_url)
            for value in response.json()['value']:
                if value[Query.project_id] in test_projects:
                    continue
                ANOResponse.append(
                    TasksSerialize(project_id=value[Query.project_id],
                                   project_name=value[Query.project_name],
                                   task_date_end=datetime_to_date(value[Query.task_date_end]),
                                   task_active=value[Query.task_active],
                                   task_name=value[Query.task_name],
                                   task_done_percent=value[Query.task_done_percent],
                                   task_date_start=datetime_to_date(value[Query.task_date_start]),
                                   task_code=value[Query.task_code],
                                   task_status=value[Query.task_status],
                                   task_base_end=datetime_to_date(value[Query.task_base_end]),
                                   task_base_start=datetime_to_date(value[Query.task_base_start]),
                                   reaper_3Q2022_plan=datetime_to_date(value[Query.reaper_3Q2022_plan]),
                                   reaper_4Q2022_plan=datetime_to_date(value[Query.reaper_4Q2022_plan]),
                                   reaper_1Q2023_plan=datetime_to_date(value[Query.reaper_1Q2023_plan]),
                                   reaper_2Q2023_plan=datetime_to_date(value[Query.reaper_2Q2023_plan]),
                                   reaper_3Q2023_plan=datetime_to_date(value[Query.reaper_3Q2023_plan]),
                                   reaper_task=value[Query.reaper_task],
                                   )
                )
            try:
                if response.json()['odata.nextLink']:
                    next_url = response.json()['odata.nextLink']
                    self.__add_params, self.table = {}, ''
                    continue
            except:
                print('All tasks downloaded successfully')
                break

        return ANOResponse


my_dict = {
    '$select': 'ИДПроекта,ИмяПроекта,'
               'НазваниеЗадачи,'
               'Кодзадачи,ДатаначалаБП0,'
               'ДатаокончанияБП0,ДатаНачалаЗадачи,'
               'ДатаОкончанияЗадачи,ПроцентЗавершенияЗадачи,'
               'Статусзадачи,ЗадачаЯвляетсяАктивной,РеперныезадачиПланIIIквартал2022,'
               'РеперныезадачиПланIVквартал2022,РеперныезадачиПланIквартал2023,'
               'РеперныезадачиПланIIквартал2023,РеперныезадачиПланIIIквартал2023,Реперныезадачи'
}


def download_data():
    ANO_Tasks = RequestAno()
    ANO_Tasks.add_params = my_dict
    data = ANO_Tasks.DataResponse()

    insert_data = []

    for i in data:
        insert_data.append((i.project_id, i.project_name,
                            i.task_code,
                            i.task_name,
                            i.task_date_start,
                            i.task_date_end,
                            i.task_done_percent,
                            i.task_base_start,
                            i.task_base_end,
                            i.task_status,
                            i.task_active,
                            i.reaper_task,
                            i.reaper_3Q2022_plan,
                            i.reaper_4Q2022_plan,
                            i.reaper_1Q2023_plan,
                            i.reaper_2Q2023_plan,
                            i.reaper_3Q2023_plan,
                            start_time
                            ))

    return insert_data