import unittest
import time
import pandas as pd
import threading

# Мультипоточна версія коду
class MultiThreadedTest(unittest.TestCase):
    def setUp(self):
        # Шлях до файлу "CAvideos.csv"
        self.file_path = "CAvideos.csv"

    def test_multi_threaded_execution(self):
        start_time = time.time()

        class DataProcessor(threading.Thread):
            def __init__(self, data):
                threading.Thread.__init__(self)
                self.data = data

            def run(self):
                # Фільтрація даних за умовою chanel_title = "EminemVEVO"
                filtered_data = self.data[self.data["channel_title"] == "Ed Sheeran"]

                # Вибір потрібних змінних
                selected_data = filtered_data[["video_id", "channel_title", "trending_date", "views", "likes", "dislikes"]]

                # Виведення даних у консоль
                print(selected_data)

        # Завантаження даних з файлу у DataFrame
        data = pd.read_csv(self.file_path)

        # Розділення даних на частини
        num_threads = 3  # Кількість потоків для обробки
        chunk_size = len(data) // num_threads
        data_chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

        # Створення та запуск потоків для обробки кожної частини даних
        processors = []
        for chunk in data_chunks:
            processor = DataProcessor(chunk)
            processors.append(processor)
            processor.start()

        # Очікування на завершення всіх потоків
        for processor in processors:
            processor.join()

        end_time = time.time()
        execution_time = end_time - start_time
        print("Multi-threaded Execution Time:", execution_time)

        self.assertGreater(100, execution_time)  # Перевірка, що час виконання менше 100 секунд

# Послідовна версія коду
class SequentialTest(unittest.TestCase):
    def setUp(self):
        # Шлях до файлу "CAvideos.csv"
        self.file_path = "CAvideos.csv"

    def test_sequential_execution(self):
        start_time = time.time()

        # Клас обробників даних
        class DataProcessor:
            def __init__(self, data):
                self.data = data

            def process_data(self):
                # Фільтрація даних за умовою chanel_title = "EminemVEVO"
                filtered_data = self.data[self.data["channel_title"] == "Ed Sheeran"]

                # Вибір потрібних змінних
                selected_data = filtered_data[["video_id", "channel_title", "trending_date", "views", "likes", "dislikes"]]

                # Виведення даних у консоль
                print(selected_data)

        # Завантаження даних з файлу у DataFrame
        data = pd.read_csv(self.file_path)

        # Розділення даних на частини
        num_processors = 3  # Кількість обробників
        chunk_size = len(data) // num_processors
        data_chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

        # Створення та обробка даних кожної частини послідовно
        processors = []
        for chunk in data_chunks:
            processor = DataProcessor(chunk)
            processors.append(processor)
            processor.process_data()

        end_time = time.time()
        execution_time = end_time - start_time
        print("Sequential Execution Time:", execution_time)

        self.assertGreater(100, execution_time)  # Перевірка, що час виконання менше 100 секунд


if __name__ == '__main__':
    unittest.main()