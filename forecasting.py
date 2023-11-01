import multiprocessing
import os
import shutil
from concurrent.futures import ProcessPoolExecutor
from threading import Thread

from pandas import concat

from tasks import (DataAggregationTask, DataAnalyzingTask, DataCalculationTask,
                   DataFetchingTask)
from utils import CITIES


def forecast_weather():
    """
    Анализ погодных условий по городам.
    """
    os.makedirs('results', exist_ok=True)

    # Создаем очередь для межпроцессного обмена данными
    queue = multiprocessing.Manager().Queue()
    fetching_task = DataFetchingTask(queue)
    calculation_task = DataCalculationTask(queue)

    processing_thread = Thread(target=calculation_task.process_queue)
    processing_thread.start()

    with multiprocessing.Pool() as pool:
        pool.map(fetching_task.get_data_and_put_to_queue, CITIES.keys())

    queue.put(None)
    processing_thread.join()

    aggregation_task = DataAggregationTask(dates=calculation_task.dates)
    aggregation_file_with_path = 'results/aggregation.json'

    # Получаем города, по которым успешно прошло вычисление погодных параметров
    valid_cities = calculation_task.cities
    with ProcessPoolExecutor(max_workers=None) as pool:
        results = list(pool.map(aggregation_task.get_dataframe, valid_cities))

    merged_results = concat(results, ignore_index=True).fillna('')
    aggregation_task.save_to_json(merged_results, aggregation_file_with_path)

    DataAnalyzingTask.make_result_from(aggregation_file_with_path)

    shutil.rmtree('results')


if __name__ == '__main__':
    forecast_weather()
