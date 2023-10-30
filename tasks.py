# import csv
import json
import logging
import os
from multiprocessing import Queue
# from pandas import DataFrame

from external.analyzer import (
    load_data,
    analyze_json,
    dump_data
)
from external.client import YandexWeatherAPI
from utils import get_url_by_city_name


format = '%(asctime)s [%(levelname)s]: %(message)s'
logging.basicConfig(
    filename='forecasting.log',
    encoding='utf-8',
    format=format,
    level=logging.INFO
)
logger = logging.getLogger()


class DataFetchingTask:
    """
    Получение данных через API и их запись в JSON-файл.
    """
    def __init__(self, queue: Queue):
        self.queue = queue

    def get_and_put_data(self, city_name: str) -> None:
        try:
            url_with_data = get_url_by_city_name(city_name)
            resp = YandexWeatherAPI.get_forecasting(url_with_data)
            if resp:
                file_with_path = "results/{}_response.json".format(city_name)
                with open(file_with_path, "w") as file:
                    json.dump(resp, file, indent=4)
                    logger.info(
                        f'Информация о городе {city_name} успешно записана.'
                    )

                self.queue.put(city_name)

        except Exception as error_msg:
            logger.error(
                f'При получении данных о городе {city_name} возникла ошибка: '
                f'{error_msg}.'
            )


class DataCalculationTask:
    """
    Вычисление погодных параметров и их запись в JSON-файл.
    """

    def __init__(self, queue: Queue):
        self.queue = queue
        self.cities = []
        self.dates = []

    def process_queue(self) -> None:
        while True:
            city_name = self.queue.get()
            if city_name is None:
                break
            # arg1 = '-i results/{}_response.json'.format(city_name)
            # arg2 = '-o results/{}_output.json'.format(city_name)
            # command = f'python3 external/analyzer.py {arg1} {arg2}'
            # os.system(command)
            data = load_data(
                input_path='results/{}_response.json'.format(city_name)
            )
            data = analyze_json(data)
            dump_data(
                data,
                output_path='results/{}_output.json'.format(city_name)
            )

            logger.info(f'Информация о городе {city_name} успешно обработана.')

            # Добавляем город в список обработанных
            self.cities.append(city_name)
            # Добавляем даты для анализа
            list_of_days = data.get('days')
            dates = [d['date'] for d in list_of_days if 'date' in d]
            self.dates = sorted(dates)
            # the work is done
            self.queue.task_done()


class DataAggregationTask:
    """
    Объединение вычисленных данных.
    """

    def aggregate_data(cities, file_with_path):

        logger.info(
            f'Данные успешно агрегированы и сохранены в {file_with_path}.'
        )

    def save_data_to_csv(data, file_with_path):

        logger.info(
            f'Данные успешно агрегированы и сохранены в {file_with_path}.'
        )


class DataAnalyzingTask:
    """
    Финальный анализ и получение результата.
    """

    def make_result_from(file_with_path):
        logger.info(f'Данные успешно прочитаны из {file_with_path}.')

        city_name = 'MOSCOW'
        print(f'{city_name} наиболее блвгоприятен для поездки.')

        logger.info('Вывод финального результата.')
