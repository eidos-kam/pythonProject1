# TODO: для классов агентов-коллекторов предусмотреть передачу в конструктор инд. параметров для Trello-коннекторов
# TODO: разделить потоки карточек для обоих источников (HTTP и БД) либо на две доски, либо на два списка
# TODO: корректно обрабатывать создание меток на доске для разных потоков данных. Сейчас они затирают метки друг друга
# TODO: добавить возможность управления работой через ключи командной строки

import datetime
from collections import deque
from os import getenv
import requests
from statistics import median, mean
import pandas as pd
from paramiko import SSHClient
import psycopg2
import yaml

HTTP_SERVER_CREDS = {"server_url": "localhost:21122/monitoring/infrastructure/using/summary/1",
                     "prices_url": "localhost:21122/monitoring/infrastructure/using/prices",
                     "generate_data_by": "ssh",  # http, ssh
                     "debug": 1}


SSH_SERVER_CREDS = {"ssh_host": "localhost",
                    "ssh_port": 2222,
                    "ssh_user": "service_user",
                    "ssh_user_password": "q1w2e3",
                    "ssh_local_key_filename": ".ssh/known_hosts",
                    "debug": 0
                    }

DB_SERVER_CREDS = {"db_type": "postgresql",
                   "db_host": "localhost",
                   "db_port": 5432,
                   "db_base": "postgres",
                   "db_scheme": "usage_stats",
                   "db_user": "postgres",
                   "db_password": "q1w2e3"
                   }

TRELLO_API_CREDS = {
                    "root_url": "https://api.trello.com/",
                    "boards_url": "1/members/me/boards",
                    "target_list": "Нужно сделать",
                    "target_board": "Управление ресурсами",
                    "debug": 1
}


class MetricsGenerator:
    """
    Запускает генерацию данных для заполнения событий в БД
    словарь параметров SSH_SERVER_CREDS
    """
    def __init__(self, server_creds: dict):
        self.__generate_data_by = 'ssh'  # определяет способ генерации данных
        self.__ssh_creds = {
            'hostname': server_creds.get('ssh_host', 'localhost'),
            'port': server_creds.get('ssh_port', 22),
            'username': server_creds.get('ssh_user', ''),
            'password': server_creds.get('ssh_user_password', '')  # словарь параметров для подключения к SSH-серверу
        }
        self.__ssh_key_filename = server_creds.get('ssh_key_filename', '.ssh/known_hosts')
        self.__debug = bool(server_creds.get('debug', 0))
        self.__generation_status = None

    @property
    def generation_status(self):
        return self.__generation_status

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def generate_data_by_ssh(self):
        with SSHClient() as ssh_client:
            ssh_client.load_system_host_keys(filename=self.__ssh_key_filename)
            ssh_client.connect(**self.__ssh_creds)
            _, _, stderr = ssh_client.exec_command("monitoring_module 1")
            # условно считаем отсутствие ошибок в stderr успешной работой скрипта-генератора
            self.__generation_status = stderr.read().decode("utf-8")
            if self.__generation_status:
                if self.__debug:
                    print("Data generated")
            else:
                raise RuntimeError(f"Failed to generate data. Error: {self.generation_status}")


class TrelloConnector:
    """
    Обеспечивает подключение к сервису Trello с параметрами key, token из переменных окружения
    позволяет создавать карточки задач и метки на конкретных досках и листах
    позволяет удалять метки
    """
    # константы сущностей Trello
    __LABELS = "labels"
    __LISTS = "lists"
    __CARDS = "cards"

    # константы HTTP-запросов
    __METHOD_POST = "POST"
    __METHOD_GET = "GET"
    __METHOD_DELETE = "DELETE"

    # доступные цвета меток в Trello (взяты из документации к Trello)
    __LABEL_COLORS = deque(["yellow", "purple", "blue", "red", "green", "orange", "black", "sky", "pink", "lime"])

    def __init__(self, trello_creds: dict):
        self.__api_key = getenv('TRELLO_KEY')   # сгенерированный Trello-key
        self.__api_token = getenv('TRELLO_TOKEN')   # сгенерированный Trello-token
        self.__root_url = trello_creds.get("root_url")  # корневой url сервиса Trello
        self.__boards_url = trello_creds.get("boards_url")  # путь до досок Trello
        self.__boards = {}    # хранит все найденные доски Trello
        self.__target_board_name = trello_creds.get("target_board")  # хранит название доски, с которой будем работать
        self.__target_board = {}  # хранит основную доску в виде словаря параметров
        self.__session = None   # хранит текущую сессию
        self.__target_list_id = None    # хранит id основного листа доски на котором будем работать
        self.__target_list_name = trello_creds.get("target_list")   # текстовое название основного листа
        self.__debug = bool(trello_creds.get("debug", 0))   # переменная-триггер для вывода отладочной информации

    @property
    def session(self):
        if self.__session is not None:
            self.__session = requests.Session()
        return self.__session

    def __enter__(self):
        """
        перегрузка контекстного менеджера для выполнения начальных действий:
        получение списка досок и установка основной доски
        получение списка листов основной доски и установка основного листа
        основные объекты - объекты (доска, список) с которыми будет проведена вся дальнейшая работа объекта
        :return:
        """
        self.__set_main_board()
        self.__set_main_list()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __get_creds_as_query_params(self):
        """
        возвращает словарь параметров, поскольку во всех запросах к Trello нужно передавать атрибуты key, token
        :return:
        """
        return {
            "key": self.__api_key,
            "token": self.__api_token
        }

    def __execute_query(self, execute_method: str, execute_url: str, execute_params: dict):
        """
        выполняет request-запрос в Trello и обрабатывает результат запроса (пытается обрабатывать)
        :param execute_method: тип запроса GET, POST, DELETE
        :param execute_url: url запроса из API Trello
        :param execute_params: словарь дополнительных параметров, необходимых для выполнения конкретного запроса
        :return: возвращает результат запроса (response) или None
        """
        params = {
            **self.__get_creds_as_query_params(),
            **execute_params
        }
        query = execute_params.get("query_type", "Unknown query")
        response = self.__session.request(method=execute_method, url=execute_url, params=params)
        if response.status_code == 200:
            print(f"Успех: <{query}> Метод: {execute_method} url: {execute_url}") if self.__debug else None
            return response
        else:
            print(f"Ошибка: <{query}> Метод: {execute_method} url:{execute_url} код: {response.status_code}")
            return None

    def __get_all_boards(self):
        """
        возвращает словарь всех доступных досок Trello
        :return: возвращает созданный словарь или None
        """
        boards = self.__execute_query(execute_method=self.__METHOD_GET,
                                      execute_url=f"{self.__root_url}{self.__boards_url}",
                                      execute_params={"query_type": "get_all_boards"}).json()
        boards = {board["name"]: {"id": board["id"]} for board in boards}
        return boards

    def delete_objects_by_name(self, object_name_iterable, object_type: str):
        """
        удаляет с основной доски объекты заданного типа (labels, cards) находя их по текстовому названию
        затем перечитывает информацию с основной доски об объектах заданного типа
        :param object_name_iterable: список текстовых названий объекта
        :param object_type: тип объекта (labels, cards)
        :return:
        """
        for object_name in object_name_iterable:
            object_id = self.__target_board[object_type].get(object_name, None)
            if object_id is not None:
                self.__execute_query(execute_method=self.__METHOD_DELETE,
                                     execute_url=f"{self.__root_url}1/{object_type}/{object_id}",
                                     execute_params={"query_type": f"delete object from {object_type}"})
        # принудительно обновить список объектов основной доски после выполнения операции
        self.__refresh_objects_list(object_type)

    def __refresh_objects_list(self, object_type: str):
        """
        принудительно обновляет список объектов основной доски (lists, labels, cards) после различны процедур
        например, удаление, добавление
        :param object_type: тип объектов для обновления (lists, labels, cards)
        :return:
        """
        self.__target_board[object_type] = self.__get_board_items(self.__target_board["id"], object_type)

    @staticmethod
    def __object_generator(iterable_object):
        """
        генератор объектов
        :param iterable_object: iterable-объект
        :return:
        """
        for item in iterable_object:
            yield item

    def __get_board_items(self, board_id, items_type: str):
        """
        возвращает в словаре объекты доски Trello в виде {{name: id},...}
        например, lists, labels, cards
        :param board_id: id доски Trello
        :param items_type: тип объекта поиска - labels или lists
        :return:
        """
        items = self.__execute_query(execute_method=self.__METHOD_GET,
                                     execute_url=f"{self.__root_url}1/boards/{board_id}/{items_type}",
                                     execute_params={"query_type": "get_board_items"}).json()
        return {item["name"]: item["id"] for item in items}

    def __set_main_board(self):
        """
        Устанавливает основной доску Trello по ее текстовому названию из параметра target_board словаря TRELLO_API_CREDS
        Все дальнейшие действия экземпляра TrelloConnector будут выполняться на этой доске
        также создает и заполняет для основной доски объекты доски lists, labels, cards
        :return: ничего не возвращает и это плохо
        """
        self.__boards = self.__get_all_boards()
        if self.__boards is not None:
            for board, board_params in self.__boards.items():
                if board == self.__target_board_name:
                    self.__target_board = {
                        "id": board_params["id"],
                        "name": board
                    }
                    self.__refresh_objects_list(self.__LISTS)
                    self.__refresh_objects_list(self.__LABELS)
                    self.__refresh_objects_list(self.__CARDS)

    def __set_main_list(self):
        """
        Устанавливает основной список доски Trello на котором будут создаваться карточки задач
        все дальнейшие действия экземпляра TrelloConnector будут выполняться в этом листе
        :return: ничего не возвращает. А надо бы
        """
        for item, item_dict in self.__target_board[self.__LISTS].items():
            if item == self.__target_list_name:
                self.__target_list_id = item_dict
                break

    def __check_object_exists_by_name(self, object_name: str, object_type: str):
        """
        проверяет существование объекта доски по его названию и типу
        :param object_name: название объекта
        :param object_type: тип объекта
        :return: возвращает True - объект существует, False - объект не существует, None - доска не доступна/не создана
        """
        return bool(object_name in self.__target_board[object_type])

    def add_cards(self, cards: list):
        """
        создает в Trello карточку задачи. Если карточка с таким названием уже есть на основном листе - она не создастся
        :param cards: список словарей подготовленных заданий с параметрами, необходимыми для создания карточек Trello
        :return:
        """
        if self.__target_board is not None and self.__target_list_id is not None:
            for card in cards:
                card_name = card["name"]
                if not self.__check_object_exists_by_name(card_name, self.__CARDS):
                    params = {
                                "name": card_name,
                                "desc": card["desc"],
                                "due": card["due"],
                                "idLabels": self.__target_board[self.__LABELS].get(card.get("label", ""), ""),
                                "idList": self.__target_list_id,
                                "query_type": "create card"
                             }
                    self.__execute_query(execute_method=self.__METHOD_POST,
                                         execute_url=f"{self.__root_url}1/cards",
                                         execute_params=params)
            self.__refresh_objects_list(self.__CARDS)

    def delete_cards_by_name(self, labels_name_iterable):
        """
        Удаляет метку с заданным текстовым именем с основной доски и перечитывает оставшиеся метки
        :param labels_name_iterable: список названий меток для удаления
        :return:
        """
        self.delete_objects_by_name(labels_name_iterable, self.__CARDS)

    def delete_cards_all(self):
        self.delete_cards_by_name(self.__object_generator(self.__target_board[self.__CARDS]))

    def add_label(self, labels: list):
        """
        создает метку с заданым названием и цветом, затем перечитывает метки основной доски
        цвет метки каруселится по списку __LABEL_COLORS
        :param labels: список текстовых наименований меток
        :return:
        """
        for label in labels:
            label_color = self.__LABEL_COLORS.popleft()
            self.__LABEL_COLORS.append(label_color)
            params = {"name": label,
                      "color": label_color,
                      "query_type": "add label"
                      }
            self.__execute_query(execute_method=self.__METHOD_POST,
                                 execute_url=f"{self.__root_url}1/boards/{self.__target_board['id']}/labels",
                                 execute_params=params)
        # обновить список меток после добавления
        self.__refresh_objects_list(self.__LABELS)

    def delete_labels_by_name(self, label_name_iterable):
        """
        Удаляет метки с заданными текстовыми именами с основной доски
        :param label_name_iterable: список названий меток, подлежащих удалению
        :return:
        """
        self.delete_objects_by_name(label_name_iterable, self.__LABELS)
        self.__refresh_objects_list(self.__LABELS)

    def delete_labels_all(self):
        """
        удаляет все метки с основной доски Trello
        :return:
        """
        self.delete_labels_by_name(self.__object_generator(self.__target_board[self.__LABELS]))


class MetricsCollectorAgent:
    """
    Собирает и парсит информацию из HTTP-источника
    """
    # константные значения заменяющие текстовые выражения числами
    _USAGE_STABLE = 0
    _USAGE_FALLING = 1
    _USAGE_RAISING = 2

    _INTENSIVITY_LOW = 10
    _INTENSIVITY_MEDIUM = 20
    _INTENSIVITY_HIGH = 30
    _INTENSIVITY_EXTREME = 40

    _DECISION_DELETE = 100
    _DECISION_NORMAL = 101
    _DECISION_OVERLOAD = 102

    _DIMENSION_CPU = 200
    _DIMENSION_RAM = 201
    _DIMENSION_NETFLOW = 202

    # словари соответствия константных числовых значений текстовым
    _DICT_USAGE_TYPE = {
        _USAGE_STABLE: 'стабильное',
        _USAGE_FALLING: 'падения',
        _USAGE_RAISING: 'скачки'
    }

    _DICT_INTENSIVITY = {
        _INTENSIVITY_LOW: 'низкая',
        _INTENSIVITY_MEDIUM: 'средняя',
        _INTENSIVITY_HIGH: 'высокая',
        _INTENSIVITY_EXTREME: 'запредельная'
    }

    _DICT_DECISION = {
        _DECISION_DELETE: 'удалить ресурс',
        _DECISION_NORMAL: 'нормальное использование',
        _DECISION_OVERLOAD: 'увеличить квоту'
    }

    # константы итоговых вычисленных метрик. Для подстановки в результирующий словарь
    _METRIC_MEAN = 1000
    _METRIC_MEDIAN = 1001
    _METRIC_USAGE_TYPE = 1002
    _METRIC_INTENSIVITY = 1003
    _METRIC_DECISION = 1004
    _METRIC_MAX_DATE = 1005
    # __METRIC_RESOURCE_COST = 1006
    __METRIC_DIMENSION_COST = 1007

    def __init__(self, server_creds: dict):
        self._request_type = 'http'  # тип/протокол запроса
        self.__url = server_creds['server_url']  # url источника данных без указания типа
        self._debug = bool(server_creds.get('debug', 0))  # переменная для режима отладки
        self.__full_url = f"{self._request_type}://{self.__url}"  # пременная с типом и запросом
        self.__prices_url = f"{self._request_type}://{server_creds['prices_url']}"  # url стоимости затрат на ресурсы
        self._response = None  # результат запроса в исхдном виде
        self._raw_data_dict = None  # результат преобразования списка с словарь
        self._aggregated_data_dict = {}  # результат обработки данных
        self.__http_session = None      # хранит экземпляр класса session для http-сессии
        self.__prices_dict = {}         # словарь стоимости затрат на наблюдаемые ресурсы

    @property
    def http_session(self):
        """
        геттер для атрибута session
        автоматически создает экземпляр session, если он не создан
        :return:
        """
        if self.__http_session is None:
            self.__http_session = requests.session()
        return self.__http_session

    def __enter__(self):
        """
        перегрузка для сбора информации из указанного источника на старте обращения к экземпляру класса
        :return: возвращает ссылку на себя
        """
        self._response = self.__http_request(url=self.__full_url)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._response = None
        print("Работа парсера завершена") if self._debug else None

    def __http_request(self, url: str, method='GET'):
        """
        выполнение http-запроса к северу по мере работы объекта класса
        :param url: ресурс доступа по HTTP
        :param method: метод доступа
        :return:  результат выполнения HTTP-запроса или исключение
        """
        response = self.http_session.request(method=method, url=url)
        if response.status_code == 200:
            return response.text
        else:
            raise RuntimeError(f"Не удалось получить данные из {url}. Ошибка: {response.status_code}")

    @staticmethod
    def _items_generator(iterable):
        """
        генератор объектов. создан, чтобы попробовать упростить код в следующей функции и потренироваться
        :param iterable: итерируемый объект
        :return:
        """
        for item in iterable:
            yield item

    @staticmethod
    def _get_data(any_data='', letter=''):
        """
        генератор для разделения исходных данных на блоки, разеделенные заданным символом (например, "$")
        для последующей обработки и преобразования в другие структуры данных
        """
        for item in any_data.split(letter):
            yield item

    @staticmethod
    def _parse_record(record=''):
        """
        разбивает одну запись данных на лексемы и возвращает список для последующей обработки
        :param record: один набор данных, содержащий информацию об одном событии из исходного набора данных
        :return: возвращает набор данных в виде списка
        """
        return record.strip("()").split(',')

    def _get_raw_data_dict(self):
        """
        преобразует с помощью генератора записи исходных данных в словарь вида
        {'command':{'resource_id':{'dimension":{'metric_unique_id':value}}}}
        metric_unique_id - для разных классов может быть разным объектом. Для HHTP - метка времени сбора информации,
        для агента БД - уникальный номер строки (id) в БД
        :return: возвращает распарсенный набор данных в виде словаря с записями по каждому событию
        """
        if self._response is not None:
            records_dict = {}
            for item in self._get_data(self._response, '$'):
                if len(item) < 2:
                    break
                team, command_metrics_records = item.split('|')
                records_dict[team] = records_dict.get(team, {})
                for record in self._get_data(command_metrics_records, ';'):
                    resource_id, resource_dimension, metric_unique_id, metric_value = self._parse_record(record)
                    records_dict[team][resource_id] = \
                        records_dict[team].get(resource_id, {})
                    records_dict[team][resource_id][resource_dimension] = \
                        records_dict[team][resource_id].get(resource_dimension, {})
                    records_dict[team][resource_id][resource_dimension][metric_unique_id] = \
                        records_dict[team][resource_id][resource_dimension].get(metric_unique_id, metric_value)
            print('Данные собраны для команд:', records_dict.keys()) if self._debug else None
            return records_dict
        else:
            return None

    def _get_data_usage_type(self, median_value: float, mean_value: float):
        """
        в соответствии условиям определяет для каждого типа аггрегированных данных состояние нагрузки
        :param median_value: медиана набора данных
        :param mean_value: среднее значение набора данных
        :return: возвращает предопределенное значение, обозначающее состояние нагрузки
        """
        divergence = mean_value / median_value
        if divergence < 0.75:
            return self._USAGE_FALLING
        elif divergence > 1.25:
            return self._USAGE_RAISING
        else:
            return self._USAGE_STABLE

    def _get_data_intensivity(self, median_value: float):
        """
        в соответствии условиям определяет для каждого типа аггрегированных данных интенсивность использования
        :param median_value: медина набора аггрегированных данных
        :return: возвращает предопределенное значение, обозначающее интенсивность использования
        """
        if 0 < median_value <= 30:
            return self._INTENSIVITY_LOW
        elif median_value <= 60:
            return self._INTENSIVITY_MEDIUM
        elif median_value <= 90:
            return self._INTENSIVITY_HIGH
        else:
            return self._INTENSIVITY_EXTREME

    def _get_usage_decision(self, intensivity, usage_type):
        """
        в соответствии условиям определяет для каждого типа аггрегированных данных текущее состояние объекта наблюдения
        :param intensivity: интенсивность использования объекта
        :param usage_type: состояние нагрузки объекта
        :return: возвращает предопределенное значение, обозначающее текущее состояние объекта наблюдения
        """
        state = intensivity + usage_type
        if 10 <= state < 20:
            return self._DECISION_DELETE
        elif state < 32:
            return self._DECISION_NORMAL
        else:
            return self._DECISION_OVERLOAD

    def __get_resource_prices(self):
        """
        опрашивает сервер на предмет информации о стоимости обслуживания наблюдаемых ресурсов по направлениям
        наблюдения (CPU, RAM, NetFlow)
        метод должен запускаться только после сбора данных о наблюдениях
        :return: возвращает словарь стоимости затрат на наблюдение за ресурсами в виде
        {resource_id: {'CPU': price, 'RAM': price, "NetFlow': price}}
        """
        yaml_prices_string = yaml.safe_load(self.__http_request(url=self.__prices_url))
        prices_dict = {}
        for resource, resource_price in yaml_prices_string["values"].items():
            prices_dict.setdefault(resource, {**resource_price})
        return prices_dict

    def get_aggregated_data_dict(self):
        """
        производит аггрегацию значений и
        создает конечный словарь с аггрегированными данными в виде структуры:
        {'command':
            {'resouce_id':
                {'dimension':
                    {'mean': value,
                     'median': value,
                     'usage_type': value,
                     'intensivity': value,
                     'decision': value
                     }
                }
            }
        }
        :return:
        """
        self._raw_data_dict = self._get_raw_data_dict()
        self.__prices_dict = self.__get_resource_prices()
        data_dict = {}
        for command in self._raw_data_dict:
            for resource_id in self._raw_data_dict[command]:
                for dimension in self._raw_data_dict[command][resource_id]:
                    values_list = []
                    dimension_cost = int(self.__prices_dict[resource_id][dimension])
                    data_dict.setdefault(command, {}).setdefault(resource_id, {}).setdefault(dimension, {})
                    for dimension_timestamp in self._raw_data_dict[command][resource_id][dimension]:
                        values_list.append(
                            float(self._raw_data_dict[command][resource_id][dimension][dimension_timestamp]))
                    tmp_dict_for_aggregated_metrics = {}
                    max_gather_date = max(self._raw_data_dict[command][resource_id][dimension])
                    max_gather_date = datetime.datetime.strptime(max_gather_date, "%Y-%m-%d %H:%M:%S").date()
                    median_value = round(float(median(values_list)), 1)
                    mean_value = round(float(mean(values_list)), 1)
                    usage_type = self._get_data_usage_type(median_value, mean_value)
                    intensivity = self._get_data_intensivity(median_value)
                    decision = self._get_usage_decision(intensivity, usage_type)
                    if self._debug:
                        print(f'get_aggregate_data_dict {command}: {resource_id}: {dimension}: '
                              f'{median_value}, {mean_value}, '
                              f'{self._DICT_USAGE_TYPE[usage_type]}, '
                              f'{self._DICT_INTENSIVITY[intensivity]}, '
                              f'{self._DICT_DECISION[decision]}')
                    tmp_dict_for_aggregated_metrics.setdefault(self._METRIC_MAX_DATE, max_gather_date)
                    tmp_dict_for_aggregated_metrics.setdefault(self._METRIC_MEAN, mean_value)
                    tmp_dict_for_aggregated_metrics.setdefault(self._METRIC_MEDIAN, median_value)
                    tmp_dict_for_aggregated_metrics.setdefault(self._METRIC_USAGE_TYPE, usage_type)
                    tmp_dict_for_aggregated_metrics.setdefault(self._METRIC_INTENSIVITY, intensivity)
                    tmp_dict_for_aggregated_metrics.setdefault(self._METRIC_DECISION, decision)
                    tmp_dict_for_aggregated_metrics.setdefault(self.__METRIC_DIMENSION_COST, dimension_cost)

                    data_dict[command][resource_id][dimension] = {**tmp_dict_for_aggregated_metrics}

        self._aggregated_data_dict = data_dict
        return data_dict

    def convert_aggregate_data_to_dataframe(self):
        """
        конвертирует полученный словарь с аггрегированными данными в pandas.DataFrame
        :return: возвращает набор данных в виде pandas.DataFrame
        """
        print('Start data converting') if self._debug else None
        if self._aggregated_data_dict is not None:
            aggregate_list = []
            for command in self._aggregated_data_dict:
                for resource_id in self._aggregated_data_dict[command]:
                    for dimension in self._aggregated_data_dict[command][resource_id]:
                        metric_value = self._aggregated_data_dict[command][resource_id][dimension]
                        aggregate_list.append([
                            command,
                            resource_id,
                            dimension,
                            metric_value[self._METRIC_MAX_DATE],
                            metric_value[self._METRIC_MEAN],
                            metric_value[self._METRIC_MEDIAN],
                            metric_value[self._METRIC_USAGE_TYPE],
                            metric_value[self._METRIC_INTENSIVITY],
                            metric_value[self._METRIC_DECISION]
                        ])
            dataframe = pd.DataFrame(aggregate_list,
                                     columns=['command', 'resource ID', 'Dimension', 'max gather date', 'mean',
                                              'mediana', 'usage type', 'intensivity', 'decision'])
            print(dataframe.head()) if self._debug else None
            return dataframe

    def print_aggregated_data(self):
        """
        выводит с учетом форматирования на экран словарь с аггрегированными данными
        """
        print('Start data printing') if self._debug else None
        if self._aggregated_data_dict is not None:
            for team in self._aggregated_data_dict:
                max_len_res_id = len(max(self._aggregated_data_dict[team]))
                max_len_dimension = len('dimension')
                max_len_usage_type = len('usage_type')
                max_len_intensivity = len('intensivity')
                print(f"\nCommand: '{team}'")
                print(
                    f"|res_id{' ' * (max_len_res_id - len('res_id'))}\t"
                    f"|Dimension\t"
                    f"|max date\t"
                    f"|mean\t"
                    f"|mediana\t"
                    f"|usage_type\t"
                    f"|intensivity\t"
                    f"|decision\t\t"
                )
                for resource_id in self._aggregated_data_dict[team]:
                    for dimension in self._aggregated_data_dict[team][resource_id]:
                        metric_value = self._aggregated_data_dict[team][resource_id][dimension]
                        blanks_usage_type = max_len_usage_type - \
                                            len(self._DICT_USAGE_TYPE[metric_value[self._METRIC_USAGE_TYPE]])
                        blanks_intensivity = max_len_intensivity - \
                                             len(self._DICT_INTENSIVITY[metric_value[self._METRIC_INTENSIVITY]])
                        print(f'|{resource_id}{" " * (max_len_res_id - len(resource_id))}\t'
                              f'|{dimension}{" " * (max_len_dimension - len(dimension))}\t'
                              f'|{metric_value[self._METRIC_MAX_DATE]}\t'
                              f'|{metric_value[self._METRIC_MEAN]}\t'
                              f'|{metric_value[self._METRIC_MEDIAN]}\t\t'
                              f'|{self._DICT_USAGE_TYPE[metric_value[self._METRIC_USAGE_TYPE]]}'
                              f'{" " * blanks_usage_type}\t'
                              f'|{self._DICT_INTENSIVITY[metric_value[self._METRIC_INTENSIVITY]]}'
                              f'{" " * blanks_intensivity}\t'
                              f'|{self._DICT_DECISION[metric_value[self._METRIC_DECISION]]}\t\t\t'
                              )

    def print_aggregated_data_with_costs(self, show_all: bool):
        """
        выводит с учетом форматирования на экран словарь с аггрегированными данными
        дополняет вывод информацией о понесенных затратах на ресурсы, подлежащие удалению
        :param: show_all: флаг, управляющий показом затрат только на удаляемые ресурсы или на все
        """
        print('Start data printing') if self._debug else None
        if self._aggregated_data_dict is not None:
            for team in self._aggregated_data_dict:
                max_len_res_id = len(max(self._aggregated_data_dict[team]))
                max_len_dimension = len('dimension')
                max_len_dimension_costs = len('dimension costs')
                max_len_usage_type = len('usage_type')
                max_len_intensivity = len('intensivity')

                print(f"\nCommand: '{team}'")
                output_header = f"|res_id{' ' * (max_len_res_id - len('res_id'))}\t" \
                                f"|dimension\t" \
                                f"|dimension costs\t" \
                                f"|max date\t" \
                                f"|mean\t" \
                                f"|mediana\t" \
                                f"|usage_type\t" \
                                f"|intensivity\t" \
                                f"|decision\t\t"
                for resource_id in self._aggregated_data_dict[team]:
                    print(output_header)
                    print("=" * len(output_header))
                    total_resource_costs = 0
                    for dimension in self._aggregated_data_dict[team][resource_id]:
                        metric_value = self._aggregated_data_dict[team][resource_id][dimension]
                        total_resource_costs += metric_value[self.__METRIC_DIMENSION_COST]
                        blanks_usage_type = abs(max_len_usage_type -
                                                len(self._DICT_USAGE_TYPE[metric_value[self._METRIC_USAGE_TYPE]]))
                        blanks_intensivity = abs(max_len_intensivity -
                                                 len(self._DICT_INTENSIVITY[metric_value[self._METRIC_INTENSIVITY]]))
                        output_line = f'|{resource_id}{" " * (max_len_res_id - len(resource_id))}\t' \
                                      f'|{dimension}{" " * (max_len_dimension - len(dimension))}\t'
                        blanks_dimension_costs = max_len_dimension_costs
                        # вывести информацию о стоимости измерения атрибута,если управленческое решение - удалить
                        if show_all or metric_value[self._METRIC_DECISION] == self._DECISION_DELETE:
                            blanks_dimension_costs = abs(blanks_dimension_costs -
                                                         len(str(metric_value[self.__METRIC_DIMENSION_COST])))
                            output_line += f'|{metric_value[self.__METRIC_DIMENSION_COST]}' \
                                           f'{" " * blanks_dimension_costs}\t'
                        else:
                            output_line += f'|{" " * blanks_dimension_costs}\t'
                        output_line += f'|{metric_value[self._METRIC_MAX_DATE]}\t' \
                                       f'|{metric_value[self._METRIC_MEAN]}\t' \
                                       f'|{metric_value[self._METRIC_MEDIAN]}\t\t' \
                                       f'|{self._DICT_USAGE_TYPE[metric_value[self._METRIC_USAGE_TYPE]]}' \
                                       f'{" " * blanks_usage_type}\t' \
                                       f'|{self._DICT_INTENSIVITY[metric_value[self._METRIC_INTENSIVITY]]}' \
                                       f'{" " * blanks_intensivity}\t' \
                                       f'|{self._DICT_DECISION[metric_value[self._METRIC_DECISION]]}\t\t\t'
                        print(output_line)
                    print(f'Total costs: {total_resource_costs}')
                    print("=" * len(output_header))

    def _compose_card(self, team, resource_id, dimension, card_data: dict, name_text: str):
        """
        подготавливает один экземпляр задачи, которая затем будет создана на доске Trello
        :param team: название команды. Нужно, чтобы на созданную карточку в Trello повесить метку команды
        :param resource_id: опрашиваемый ресурс
        :param dimension: наблюдаемая метрика ресурса
        :param card_data: словарь с дополнительными полями по наблюдаемой метрике ресурса
        :param name_text: текстовое дополнение для формирования шаблона названия карточки
        :return: возвращает подготовленную для создания карточки Trello задачу
        """
        return {
            "name": f"{name_text} ресурса {resource_id} по измерению {dimension}",
            "desc": f"Использование ресурса: {self._DICT_USAGE_TYPE[card_data[self._METRIC_USAGE_TYPE]]} "
                    f"Интенсивность использования: {self._DICT_INTENSIVITY[card_data[self._METRIC_INTENSIVITY]]}",
            "due": card_data[self._METRIC_MAX_DATE] + datetime.timedelta(days=14),
            "label": team
        }

    def create_task_list_from_metrics(self):
        """
        создает из данных, полученных из БД список (list) готовых задач для дальнейшего создания карточек Trello
        :return:  возвращает подготовленный список задач
        """
        if self._aggregated_data_dict is not None:
            cards_to_create = []
            for team, team_record in self._aggregated_data_dict.items():
                print(team)
                for resource_id, dimensions in team_record.items():
                    print(" -->", resource_id) if self._debug else None
                    for dimension, metrics in dimensions.items():
                        print("----->", dimension, ":", metrics) if self._debug else None
                        if metrics[self._METRIC_DECISION] == self._DECISION_DELETE:
                            decision = "Отказаться от использования"
                        elif metrics[self._METRIC_DECISION] == self._DECISION_OVERLOAD:
                            decision = "Увеличить квоты"
                        else:
                            continue
                        cards_to_create.append(self._compose_card(team, resource_id, dimension, metrics, decision))
            return cards_to_create
        else:
            return []

    def create_labels_for_teams(self):
        """
        удаляет все ранее созданные метки в Trello и создает их заново по названиям команд (teams)
        :return:
        """
        with TrelloConnector(TRELLO_API_CREDS) as trello_connector:
            trello_connector.delete_labels_all()
            trello_connector.add_label(list(self._aggregated_data_dict))

    def create_cards(self):
        """
        создает в Trello карточки-задания
        :return:
        """
        cards = self.create_task_list_from_metrics()
        with TrelloConnector(TRELLO_API_CREDS) as trello_connector:
            trello_connector.add_cards(list(self._items_generator(cards)))
#             for card in cards:
#                 trello_connector.add_card(card)


class MetricsCollectorAgentPostgres(MetricsCollectorAgent):
    """
    Собирает и парсит информацию из Базы данных PostgreSQL
    """

    def __init__(self, server_creds: dict, db_creds: dict):
        super().__init__(server_creds)
        self.__request_type = 'postgres'  # тип запроса
        self.__db_creds = {'host': db_creds.get('db_host'),
                           'port': db_creds.get('db_port'),
                           'dbname': db_creds.get('db_base'),
                           'user': db_creds.get('db_user'),
                           'password': db_creds.get('db_password')
                           }

    def __enter__(self):
        """
        перегрузка для сбора информации из указанного источника на старте обращения к экземпляру класса
        :return: возвращает ссылку на себя
        """
        self.__get_data_from_database()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._response = None
        print("Работа парсера завершена") if self._debug else None

    def __get_data_from_database(self):
        """
        Получает из базы набор данных в требуемом формате. Создает словарь с данными для дальнейшей обработки
        :return: ничего не возвращает, хотя надо бы обработать результат(статус) запроса в БД
                устанавливает переменную self._aggregated_data_dict
        """
        with psycopg2.connect(**self.__db_creds) as db_conn:
            with db_conn.cursor() as db_cursor:
                db_cursor.execute(R"""
                                    with metrics_raw_data as
                                    (
                                    select 
                                        r.id,
                                        r.team,
                                        r.resource,
                                        r.dimension,
                                        r.collect_date,
                                        r.usage
                                    from
                                        usage_stats.resources r
                                    group by 2, 3, 4, 1
                                    ), metrics_aggregated_data as
                                    (
                                    select 
                                        mrd.team,
                                        mrd.resource, 
                                        mrd.dimension,
                                        max(mrd.collect_date) max_date,
                                        percentile_cont(0.5) within group (order by mrd.usage) as mediana,
                                        avg(mrd.usage) as average
                                    from
                                        metrics_raw_data mrd
                                    group by 1, 2, 3
                                        )
                                    select 
                                        mad.team, 
                                        mad.resource, 
                                        mad.dimension, 
                                        mad.max_date,
                                        mad.mediana,
                                        mad.average
                                    from metrics_aggregated_data mad
                                        """)
                data_dict = {}
                while True:
                    team_records = db_cursor.fetchmany(1000)
                    if not team_records:
                        break
                    for record in team_records:
                        tmp_dict = {}
                        team, resource_id, dimension, max_date, dimension_median, dimension_average = record
                        intensivity = self._get_data_intensivity(dimension_median)
                        usage_type = self._get_data_usage_type(dimension_median, dimension_average)
                        decision = self._get_usage_decision(intensivity, usage_type)
                        # print(team, resource_id, dimension, dimension_median, dimension_average)
                        data_dict.setdefault(team, {}).setdefault(resource_id, {}).setdefault(dimension, {})
                        # data_dict[team][resource_id] = data_dict[team].get(resource_id, {})
                        # data_dict[team][resource_id][dimension] = data_dict[team][resource_id].get(dimension, {})
                        if self._debug:
                            print(f'get_aggregate_data_dict {team}: {resource_id}: {dimension}: '
                                  f'{dimension_median}, {dimension_average}, '
                                  f'{self._DICT_USAGE_TYPE[usage_type]}, '
                                  f'{self._DICT_INTENSIVITY[intensivity]}, '
                                  f'{self._DICT_DECISION[decision]}')
                        # метрика максмальная дата сбора сведений
                        tmp_dict.setdefault(self._METRIC_MAX_DATE, max_date)
                        # метрика среднее значение
                        tmp_dict.setdefault(self._METRIC_MEAN, dimension_average)
                        # метрика значение медианы
                        tmp_dict.setdefault(self._METRIC_MEDIAN, dimension_median)
                        # метрика степень использования
                        tmp_dict.setdefault(self._METRIC_USAGE_TYPE, usage_type)
                        # метрика интенсивность нагрузки
                        tmp_dict.setdefault(self._METRIC_INTENSIVITY, intensivity)
                        # управленческое решение
                        tmp_dict.setdefault(self._METRIC_DECISION, decision)
                        data_dict[team][resource_id][dimension] = {**tmp_dict}
                self._aggregated_data_dict = data_dict


def main():
    print("Start")
    # примеры использования всех классов
    # with TrelloConnector(TRELLO_API_CREDS) as trello:
    #    trello.delete_cards_all()
    #    trello.delete_labels_all()
    # with MetricsGenerator(SSH_SERVER_CREDS) as mg:
    #     mg.generate_data_by_ssh()
    # with MetricsCollectorAgentPostgres(HTTP_SERVER_CREDS, DB_SERVER_CREDS) as mca_postgres_agent:
        # mca_postgres_agent.create_labels_for_teams()
        # mca_postgres_agent.create_cards()
    #     mca_postgres_agent.print_aggregated_data()
    with MetricsCollectorAgent(HTTP_SERVER_CREDS) as mca_http_agent:
        mca_http_agent.get_aggregated_data_dict()
        mca_http_agent.print_aggregated_data_with_costs(show_all=False)
    print("Stop")


if __name__ == '__main__':
    main()
