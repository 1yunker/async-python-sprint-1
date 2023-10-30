# import subprocess
from itertools import count
import json
import multiprocessing
import numpy
# import shutil

# from concurrent.futures import ThreadPoolExecutor
from pandas import DataFrame
from threading import Thread
from tasks import (
    # DataAggregationTask,
    DataAnalyzingTask,
    DataCalculationTask,
    DataFetchingTask,
)
from utils import CITIES


def forecast_weather():
    """
    Анализ погодных условий по городам.
    """

    # Создаем очередь для городов с валидными данными
    queue = multiprocessing.Manager().Queue()
    fetching_task = DataFetchingTask(queue)
    calculation_task = DataCalculationTask(queue)

    processing_thread = Thread(target=calculation_task.process_queue)
    processing_thread.start()

    with multiprocessing.Pool() as pool:
        pool.map(fetching_task.get_and_put_data, CITIES.keys())

    queue.put(None)
    processing_thread.join()

    # Получаем города, по которым успешно прошло вычисление погодных параметров
    # valid_cities = calculation_task.cities
    # print(list(valid_cities))

    columns = []
    columns.append('City')
    columns.append('Condition')
    columns.extend(calculation_task.dates)
    columns.append('Avg')
    # columns.append('Rating')

    df = DataFrame(columns=columns)
    for city_name in calculation_task.cities:
        with open("results/{}_output.json".format(city_name), "r") as file:
            data_read = json.load(file).get('days')
            lst_temps = []
            lst_hours = []
            for item in data_read:
                lst_temps.append(item.get('temp_avg'))
                lst_hours.append(item.get('relevant_cond_hours'))

            lst = [city_name, 'temp_avg']
            lst.extend(lst_temps)
            lst.append(numpy.average([d for d in lst_temps if d is not None]))
            df.loc[len(df)] = lst
            lst = [city_name, 'relevant_cond_hours']
            lst.extend(lst_hours)
            lst.append(numpy.average([d for d in lst_hours if d is not None]))
            df.loc[len(df)] = lst

    df.to_json('results/aggregation.json', orient='records', indent=4)

    # file_with_path = '/results/aggregation.csv'
    # DataAggregationTask.aggregate_data_to_csv(valid_cities, file_with_path)
    # DataAnalyzingTask.make_result_from(file_with_path)

    # shutil.rmtree('results')


if __name__ == '__main__':
    forecast_weather()
