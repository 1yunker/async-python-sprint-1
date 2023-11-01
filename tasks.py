import json
import logging
from multiprocessing import Queue

from numpy import average
from pandas import DataFrame

from external.analyzer import analyze_json, dump_data, load_data
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
        self._queue = queue

    def get_data_and_put_to_queue(self, city_name: str) -> None:
        try:
            url_with_data = get_url_by_city_name(city_name)
            resp = YandexWeatherAPI.get_forecasting(url_with_data)
            if resp:
                file_with_path = 'results/{}_response.json'.format(city_name)
                with open(file_with_path, 'w') as file:
                    json.dump(resp, file, indent=4)
                    logger.info(
                        f'Информация о городе {city_name} успешно записана.'
                    )

                self._queue.put(city_name)

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
        self._queue = queue
        self.cities = []
        self.dates = []

    def process_queue(self) -> None:
        while True:
            city_name = self._queue.get()
            if city_name is None:
                break

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
            # Добавляем даты в список для анализа
            list_of_days = data.get('days')
            dates = [d['date'] for d in list_of_days if 'date' in d]
            self.dates = sorted(dates)
            # Подаем сигнал что задача выполнена
            self._queue.task_done()


class DataAggregationTask:
    """
    Объединение вычисленных данных и их запись в JSON-файл..
    """
    def __init__(self, dates: list):
        self.dates = dates

    def get_dataframe(self, city_name: str) -> DataFrame:
        # Инициализируем dataframe
        columns = []
        columns.append('City')
        columns.append('Condition')
        columns.extend(self.dates)
        columns.append('Avg')
        df = DataFrame(columns=columns)

        with open('results/{}_output.json'.format(city_name), 'r') as file:
            data = json.load(file).get('days')

            lst_temps = []
            lst_hours = []
            for ev in data:
                lst_temps.append(ev.get('temp_avg'))
                lst_hours.append(ev.get('relevant_cond_hours'))

            lst = [city_name, 'temp_avg']
            lst.extend(lst_temps)
            lst.append(average([d for d in lst_temps if d is not None]))
            df.loc[len(df)] = lst

            lst = [city_name, 'relevant_cond_hours']
            lst.extend(lst_hours)
            lst.append(average([d for d in lst_hours if d is not None]))
            df.loc[len(df)] = lst

            logger.info(f'Данные по {city_name} успешно упакованы в dataframe')

        return df

    def save_to_json(self, df: DataFrame, file_with_path: str) -> None:
        df.to_json(file_with_path, orient='records', indent=4)
        logger.info(
            f'Данные успешно агрегированы и сохранены в {file_with_path}.'
        )


class DataAnalyzingTask:
    """
    Финальный анализ агрегированных данных и получение результата.
    """

    def make_result_from(file_with_path: str) -> None:

        with open(file_with_path, 'r') as file:
            logger.info(f'Данные успешно прочитаны из {file_with_path}.')
            data = json.load(file)

        max_temp_avg = max(
            [ev['Avg'] for ev in data if ev['Condition'] == 'temp_avg']
        )
        data_with_max_temp_avg = list(
            filter(lambda d: d['Avg'] == max_temp_avg, data)
        )
        if len(data_with_max_temp_avg) > 1:
            cities_with_max_temp = []
            for ev in data_with_max_temp_avg:
                cities_with_max_temp.append(ev['City'])

            max_relevant_cond_hours = max(
                [ev['Avg'] for ev in data
                 if ev['Condition'] == 'relevant_cond_hours'
                 and ev['City'] in cities_with_max_temp]
            )
            data_with_max_relevant_cond_hours = list(
                filter(lambda d: d['Avg'] == max_relevant_cond_hours, data)
            )
            if len(data_with_max_relevant_cond_hours) > 1:
                cities_with_max_relevant_cond_hours = []
                for i in data_with_max_relevant_cond_hours:
                    cities_with_max_relevant_cond_hours.append(i['City'])
                cities = ', '.join(cities_with_max_relevant_cond_hours)
                print(f'{cities} наиболее благоприятны для поездки.')
            else:
                city_name = data_with_max_relevant_cond_hours[0].get('City')
                print(f'{city_name} наиболее благоприятен для поездки.')
        else:
            city_name = data_with_max_temp_avg[0].get('City')
            print(f'{city_name} наиболее благоприятен для поездки.')

        logger.info('Вывод финального результата.')
