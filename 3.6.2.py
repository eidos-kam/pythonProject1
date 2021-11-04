import requests
from statistics import median, mean
import pandas as pd

# from IPython.display import display

SERVER_CREDS = {"server_url": "localhost:21122/monitoring/infrastructure/using/summary/1",
                "request_type": "http",
                "debug": 0}


class MetricsCollectorAgent:
    """
    Собирает и парсит информацию из источника
    """
    # константные значения заменяющие текстовые выражения числами
    __USAGE_STABLE = 0
    __USAGE_FALLING = 1
    __USAGE_RAISING = 2

    __INTENSIVITY_LOW = 10
    __INTENSIVITY_MEDIUM = 20
    __INTENSIVITY_HIGH = 30
    __INTENSIVITY_EXTREME = 40

    __DECISION_DELETE = 100
    __DECISION_NORMAL = 101
    __DECISION_OVERLOAD = 102

    # словари соответствия константных числовых значений текстовым
    __DICT_USAGE_TYPE = {
        __USAGE_STABLE: 'stable ',
        __USAGE_FALLING: 'falling',
        __USAGE_RAISING: 'raising'
    }

    __DICT_INTENSIVITY = {
        __INTENSIVITY_LOW: 'low    ',
        __INTENSIVITY_MEDIUM: 'medium ',
        __INTENSIVITY_HIGH: 'high   ',
        __INTENSIVITY_EXTREME: 'extreme'
    }

    __DICT_DECISION = {
        __DECISION_DELETE: 'delete resource',
        __DECISION_NORMAL: 'normal usage',
        __DECISION_OVERLOAD: 'extend resource'
    }

    # константы итоговых вычисленных метрик. Для подстановки в результирующий словарь
    __METRIC_MEAN = 1000
    __METRIC_MEDIAN = 1001
    __METRIC_USAGE_TYPE = 1002
    __METRIC_INTENSIVITY = 1003
    __METRIC_DECISION = 1004

    def __init__(self, server_creds: dict):
        self.__request_type = server_creds.get('request_type', 'http')  # тип запроса
        self.__url = server_creds['server_url']  # url источника данных без указания типа
        self.__debug = bool(server_creds.get('debug', 0))  # переменная для режима отладки
        self.__full_url = f"{self.__request_type}://{self.__url}"  # пременная с типом и запросом
        self.__response = None  # результат запроса в исхдном виде
        # self.__raw_data_list = None  # результат запроса преобразованный в список
        self.__raw_data_dict = None  # результат преобразования списка с словарь
        self.__aggregate_data_dict = None  # результат обработки данных

    def __enter__(self):
        """
        перегрузка для сбора информации из указанного источника на старте обращения к экземпляру класса
        :return: возвращает ссылку на себя
        """
        if self.__response is None:
            self.__response = requests.get(self.__full_url)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__response = None
        print("Работа парсера завершена") if self.__debug else None

    def __get_data(self, any_data, letter: str):
        """
        генератор для разделения исходных данных на блоки, разеделенные заданным символом (например, "$")
        для последующей обработки и преобразования в другие структуры данных
        """
        for item in any_data.split(letter):
            yield item

    def __parse_record(self, record: str):
        """
        разбивает одну запись данных на лексемы и возвращает список для последующей обработки
        :param record: один набор данных, содержащий информацию об одном событии из исходного набора данных
        :return: возвращает набор данных в виде списка
        """
        return record.strip("()").split(',')

    def __get_raw_data_dict(self):
        """
        преобразует с помощью генератора записи исходных данных в словарь вида
        {'command':{'resource_id':{'dimension":{'timestamp':value}}}}
        :return: возвращает распарсенный набор данных в виде словаря с записями по каждому событию
        """
        if self.__response is not None:
            records_dict = {}
            for item in self.__get_data(self.__response.text, '$'):
                command, command_metrics_records = item.split('|')
                records_dict[command] = records_dict.get(command, {})
                for record in self.__get_data(command_metrics_records, ';'):
                    resource_id, resource_metric, metric_timestamp, metric_value = self.__parse_record(record)
                    records_dict[command][resource_id] = \
                        records_dict[command].get(resource_id, {})
                    records_dict[command][resource_id][resource_metric] = \
                        records_dict[command][resource_id].get(resource_metric, {})
                    records_dict[command][resource_id][resource_metric][metric_timestamp] = \
                        records_dict[command][resource_id][resource_metric].get(metric_timestamp, metric_value)
            else:
                print('Данные собраны для команд:', records_dict.keys()) if self.__debug else None
                return records_dict
        else:
            return None

    def __get_data_usage_type(self, median_value: float, mean_value: float):
        """
        в соответствии условиям определяет для каждого типа аггрегированных данных состояние нагрузки
        :param median_value: медиана набора данных
        :param mean_value: среднее значение набора данных
        :return: возвращает предопределенное значение, обозначающее состояние нагрузки
        """
        divergence = mean_value / median_value
        if divergence < 0.75:
            return self.__USAGE_FALLING
        elif divergence > 1.25:
            return self.__USAGE_RAISING
        else:
            return self.__USAGE_STABLE

    def __get_data_intensivity(self, median_value: float):
        """
        в соответствии условиям определяет для каждого типа аггрегированных данных интенсивность использования
        :param median_value: медина набора аггрегированных данных
        :return: возвращает предопределенное значение, обозначающее интенсивность использования
        """
        if 0 < median_value <= 30:
            return self.__INTENSIVITY_LOW
        elif median_value <= 60:
            return self.__INTENSIVITY_MEDIUM
        elif median_value <= 90:
            return self.__INTENSIVITY_HIGH
        else:
            return self.__INTENSIVITY_EXTREME

    def __get_usage_decision(self, intensivity, usage_type):
        """
        в соответствии условиям определяет для каждого типа аггрегированных данных текущее состояние объекта наблюдения
        :param intensivity: интенсивность использования объекта
        :param usage_type: состояние нагрузки объекта
        :return: возвращает предопределенное значение, обозначающее текущее состояние объекта наблюдения
        """
        state = intensivity + usage_type
        if 10 <= state <= 20:
            return self.__DECISION_DELETE
        elif state <= 31:
            return self.__DECISION_NORMAL
        else:
            return self.__DECISION_OVERLOAD

    def get_aggregate_data_dict(self):
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
        if self.__raw_data_dict is None:
            self.__raw_data_dict = self.__get_raw_data_dict()
            data_dict = {}
            for command in self.__raw_data_dict.keys():
                data_dict[command] = data_dict.get(command, {})
                for resource_id in self.__raw_data_dict[command].keys():
                    data_dict[command][resource_id] = data_dict[command].get(resource_id, {})
                    for dimension in self.__raw_data_dict[command][resource_id].keys():
                        data_dict[command][resource_id][dimension] = data_dict[command][resource_id].get(dimension, {})
                        values_list = []
                        for dimension_timestamp in self.__raw_data_dict[command][resource_id][dimension].keys():
                            values_list.append(
                                int(self.__raw_data_dict[command][resource_id][dimension][dimension_timestamp]))
                        else:
                            median_value = round(float(median(values_list)), 1)
                            mean_value = round(float(mean(values_list)), 1)
                            usage_type = self.__get_data_usage_type(median_value, mean_value)
                            intensivity = self.__get_data_intensivity(median_value)
                            decision = self.__get_usage_decision(intensivity, usage_type)
                            if self.__debug:
                                print(f'get_aggregate_data_dict {command}: {resource_id}: {dimension}: '
                                      f'{median_value}, {mean_value}, '
                                      f'{self.__DICT_USAGE_TYPE[usage_type]}, '
                                      f'{self.__DICT_INTENSIVITY[intensivity]}, '
                                      f'{self.__DICT_DECISION[decision]}')
                            data_dict[command][resource_id][dimension][self.__METRIC_MEAN] = \
                                data_dict[command][resource_id][dimension].get(self.__METRIC_MEAN, mean_value)

                            data_dict[command][resource_id][dimension][self.__METRIC_MEDIAN] = \
                                data_dict[command][resource_id][dimension].get(self.__METRIC_MEDIAN, median_value)

                            data_dict[command][resource_id][dimension][self.__METRIC_USAGE_TYPE] = \
                                data_dict[command][resource_id][dimension].get(self.__METRIC_USAGE_TYPE, usage_type)

                            data_dict[command][resource_id][dimension][self.__METRIC_INTENSIVITY] = \
                                data_dict[command][resource_id][dimension].get(self.__METRIC_INTENSIVITY, intensivity)

                            data_dict[command][resource_id][dimension][self.__METRIC_DECISION] = \
                                data_dict[command][resource_id][dimension].get(self.__METRIC_DECISION, decision)
            else:
                self.__aggregate_data_dict = data_dict
                return data_dict

    def convert_aggregate_data_to_dataframe(self):
        """
        конвертирует полученный словарь с аггрегированными данными в pandas.DataFrame
        :return: возвращает набор данных в виде pandas.DataFrame
        """
        print('Start data converting') if self.__debug else None
        if self.__aggregate_data_dict is not None:
            aggregate_list = []
            for command in self.__aggregate_data_dict.keys():
                for resource_id in self.__aggregate_data_dict[command].keys():
                    for dimension in self.__aggregate_data_dict[command][resource_id]:
                        metric_value = self.__aggregate_data_dict[command][resource_id][dimension]
                        aggregate_list.append([
                            command,
                            resource_id,
                            dimension,
                            metric_value[self.__METRIC_MEAN],
                            metric_value[self.__METRIC_MEDIAN],
                            metric_value[self.__METRIC_USAGE_TYPE],
                            metric_value[self.__METRIC_INTENSIVITY],
                            metric_value[self.__METRIC_DECISION]
                        ])
            dataframe = pd.DataFrame(aggregate_list,
                                     columns=['command', 'resource ID', 'Dimension', 'mean', 'mediana', 'usage type',
                                              'intensivity', 'decision'])
            print(dataframe.head()) if self.__debug else None
            return dataframe

    def print_aggregated_data(self):
        """
        выводит с учетом форматирования на экран словарь с аггрегированными данными
        """
        print('Start data printing') if self.__debug else None
        if self.__aggregate_data_dict is not None:
            for command in self.__aggregate_data_dict.keys():
                max_len_res_id = len(max(self.__aggregate_data_dict[command].keys()))
                max_len_dimension = len('dimension')
                print(f"\nCommand: '{command}'")
                print(
                    f"|res_id{' ' * (max_len_res_id - len('res_id'))}\t"
                    f"|Dimension\t"
                    f"|mean\t"
                    f"|mediana\t"
                    f"|usage_type\t"
                    f"|intensivity\t"
                    f"|decision\t\t"
                )
                for resource_id in self.__aggregate_data_dict[command].keys():
                    for dimension in self.__aggregate_data_dict[command][resource_id]:
                        metric_value = self.__aggregate_data_dict[command][resource_id][dimension]
                        print(f'|{resource_id}{" " * (max_len_res_id - len(resource_id))}\t'
                              f'|{dimension}{" " * (max_len_dimension - len(dimension))}\t'
                              f'|{metric_value[self.__METRIC_MEAN]}\t'
                              f'|{metric_value[self.__METRIC_MEDIAN]}\t\t'
                              f'|{self.__DICT_USAGE_TYPE[metric_value[self.__METRIC_USAGE_TYPE]]}\t'
                              f'|{self.__DICT_INTENSIVITY[metric_value[self.__METRIC_INTENSIVITY]]}\t\t'
                              f'|{self.__DICT_DECISION[metric_value[self.__METRIC_DECISION]]}\t\t\t'
                              )


def main():
    print("Start")
    with MetricsCollectorAgent(SERVER_CREDS) as cursor:
        cursor.get_aggregate_data_dict()
        print('Data aggregated')
        cursor.print_aggregated_data()
        print('data converted')
    print("Stop")


if __name__ == '__main__':
    main()
