import os
from dotenv import load_dotenv
# import nltk
# nltk.download('stopwords')
from nltk.corpus import stopwords

def read_cities_from_file(filename):
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            cities = [city.strip().lower() for city in file.readlines()]
            return cities
    except FileNotFoundError:
        print(f"Файл {filename} не найден.")
        return []


load_dotenv()

xml_user = os.getenv("XML_USER")
xml_key = os.getenv("XML_KEY")
google_api_key = os.getenv("GOOGLE_API_KEY")
DATABASE_URL = "sqlite+aiosqlite:///database.sqlite"
russian_stop_words = set(stopwords.words('russian'))
cities = read_cities_from_file("cities.txt")
russian_stop_words.update(cities)
