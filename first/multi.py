import pandas as pd
import threading

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

# Шлях до файлу "CAvideos.csv"
file_path = "CAvideos.csv"

# Завантаження даних з файлу у DataFrame
data = pd.read_csv(file_path)

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