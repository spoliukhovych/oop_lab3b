import pandas as pd

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

# Шлях до файлу "CAvideos.csv"
file_path = "CAvideos.csv"

# Завантаження даних з файлу у DataFrame
data = pd.read_csv(file_path)

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