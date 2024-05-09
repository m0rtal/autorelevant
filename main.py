import sys
import os
import re

from bs4 import BeautifulSoup
from fastapi import FastAPI, Query, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, ForeignKey
import ssl
from aiohttp import TCPConnector
import loguru
import aiohttp
import asyncio
import xml.etree.ElementTree as ET
from collections import Counter
import pandas as pd
from string import punctuation

import nltk
from nltk.corpus import stopwords

nltk.download('stopwords')
russian_stop_words = set(stopwords.words('russian'))

from joblib import Parallel, delayed

from pymystem3 import Mystem

mystem = Mystem()

from dotenv import load_dotenv

load_dotenv()

xml_user = os.getenv("XML_USER")
xml_key = os.getenv("XML_KEY")

# База данных и модель
DATABASE_URL = "sqlite+aiosqlite:///database.sqlite"
Base = declarative_base()


class Database:
    def __init__(self, database_url):
        self.engine = create_async_engine(database_url, echo=False, connect_args={"timeout": 15})
        self.async_session = sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)
        self.Base = Base

    async def create_all(self):
        async with self.engine.begin() as conn:
            try:
                await conn.run_sync(self.Base.metadata.create_all)
                loguru.logger.info("Tables created successfully")
            except Exception as e:
                loguru.logger.error(f"Error creating tables: {e}")
                raise e

    async def save_request(self, url: str, search_string: str, region: str, domain: str):
        async with self.async_session() as session:
            try:
                async with session.begin():
                    db_request = UserRequest(url=url, search_string=search_string, region=region, domain=domain)
                    session.add(db_request)
                await session.commit()
                loguru.logger.info(f"Request saved: {db_request}")
                return db_request
            except Exception as e:
                loguru.logger.error(f"Error saving request: {e}")
                raise e

    async def save_search_results(self, request_id: int, urls: list):
        async with self.async_session() as session:
            try:
                async with session.begin():
                    results = [SearchResult(request_id=request_id, url=url) for url in urls]
                    session.add_all(results)
                await session.commit()
                loguru.logger.info(f"Search results saved: {len(results)} items")
            except Exception as e:
                loguru.logger.error(f"Error saving search results: {e}")
                raise e

    async def save_page_contents(self, request_id: int, contents: dict):
        async with self.async_session() as session:
            try:
                async with session.begin():
                    page_contents = [
                        PageContent(request_id=request_id, url=url, content=content)
                        for url, content in contents.items() if content is not None
                    ]
                    session.add_all(page_contents)
                await session.commit()
                logger.info(f"Page contents saved: {len(page_contents)} items")
            except Exception as e:
                logger.error(f"Error saving page contents: {e}")
                raise e


class UserRequest(Base):
    __tablename__ = "requests"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    url = Column(String)
    search_string = Column(String)
    region = Column(String)
    domain = Column(String)


class SearchResult(Base):
    __tablename__ = "search_results"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    request_id = Column(Integer, ForeignKey("requests.id"), nullable=False)
    url = Column(String, nullable=False)


class PageContent(Base):
    __tablename__ = "page_content"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    request_id = Column(Integer, ForeignKey("requests.id"), nullable=False)
    url = Column(String, nullable=False)
    content = Column(String, nullable=False)


# Настройка логгера
logger = loguru.logger
logger.add("log/app.log", level="INFO", rotation="10 MB", compression="zip")
logger.add(sys.stderr, level="WARNING")


async def startup():
    # Создание таблиц, если они еще не созданы
    database = Database(DATABASE_URL)
    await database.create_all()


# FastAPI app
app = FastAPI()
app.add_event_handler("startup", startup)


def mock_process_request(request):
    """Обработчик для имитации обработки запроса."""
    logger.info(f"Processing request with ID: {request.id}, URL: {request.url}")
    return {"message": "Request processed", "request_id": request.id}


async def parse_xml(xml_string):
    root = ET.fromstring(xml_string)
    urls = []
    for doc in root.findall('.//doc'):
        url = doc.find('url').text
        urls.append(url)
    return urls


async def yandex_xmlproxy_request(search_string: str, region: str, user_id: str = xml_user, api_key: str = xml_key):
    url = 'https://xmlstock.com/yandex/xml/'

    params = {
        'user': user_id,
        'key': api_key,
        'query': search_string,
        'lr': region,
        'groupby': 'mode=flat.groups-on-page=100.docs-in-group=1'
    }

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    logger.error(f"Yandex XMLProxy request error: HTTP status code {response.status}")
                    return None

                content = await response.text()
                logger.info(f"Yandex XMLProxy request successful. Received {len(content)} bytes.")
                return await parse_xml(content)
        except aiohttp.ClientError as e:
            logger.error(f"Yandex XMLProxy request error: {e}")
            return None


async def google_proxy_request(search_string: str, location: str, domain: str):
    url = 'https://api.spaceserp.com/google/search'

    params = {
        'apiKey': '0d421ceb-820a-418c-a1dd-cdcb9210317c',
        'q': search_string,
        'location': location,
        'domain': domain,
        'gl': 'ru',
        'hl': 'ru',
        'resultFormat': 'json'
    }

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    logger.error(f"Google SERP API request error: HTTP status code {response.status}")
                    return None

                content = await response.json()
                logger.info(f"Google SERP API request successful. Received {len(content)} bytes.")
                return content['organic_results']
        except aiohttp.ClientError as e:
            logger.error(f"Google SERP API request error: {e}")
            return None


def load_stop_words(file_path: str) -> set:
    """Загружает стоп-слова из указанного файла."""
    stop_words = set()
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            stop_words.add(line.strip())
    return stop_words


def filter_urls(urls: list, stop_words: set) -> list:
    """Фильтрует URL-адреса по стоп-словам."""
    filtered_urls = []
    for url in urls:
        if not any(stop_word in url for stop_word in stop_words):
            filtered_urls.append(url)
    return filtered_urls


async def process_urls(urls: list):
    page_contents = {}
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
        tasks = []
        for url in urls:
            task = asyncio.create_task(fetch_page_content(session, url))
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        for url, content in results:
            if content:
                page_contents[url] = content
    return page_contents


async def fetch_page_content(session, url: str):
    logger.info(f"Обрабатываем страницу {url}...")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        # Создание SSL контекста, который не проверяет сертификат
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        # Использование кастомного TCPConnector с модифицированным SSL контекстом
        async with session.get(url, ssl=ssl_context, headers=headers) as response:
            if response.status == 200:  # Проверка статуса HTTP ответа
                content = await response.text()
                soup = BeautifulSoup(content, "html.parser")
                for script_or_style in soup(["script", "style"]):
                    script_or_style.decompose()
                page_content = soup.get_text(separator=" ").strip()
                page_content = re.sub(r"\s+", " ", page_content)
                page_content = page_content.encode("utf-8", errors="replace").decode("utf-8", errors="replace")
                logger.info(f"Обработка страницы {url} завершена успешно")
                return (url, page_content)  # Возвращаем кортеж (url, page_content)
            else:
                logger.warning(f"HTTP status code {response.status} for URL {url}")
                return (url, None)  # Возвращаем None, если статус не 200
    except Exception as e:
        logger.error(f"Error fetching URL {url}: {e}")
        return (url, None)  # Возвращаем None для контента в случае ошибки


def lemmatize_text(text):
    # Удаление пунктуации
    text = re.sub(r'[' + punctuation + ']', '', text)
    # Удаление цифр
    text = re.sub(r'\d+', '', text)

    lemmas = mystem.lemmatize(text.lower())

    # Получаем леммы для каждого токена в тексте, исключая стоп-слова
    lemmas = [lemma for lemma in lemmas if lemma not in russian_stop_words and lemma.strip()]
    return lemmas


def get_lemmatized_words(contents):
    # Использование Parallel и delayed из joblib для параллельной обработки
    lemmatized_words = Parallel(n_jobs=-1)(delayed(lemmatize_text)(content) for content in contents)
    return lemmatized_words


async def get_median_lemmatized_word_frequency(contents):
    # Синхронный вызов асинхронной функции
    loop = asyncio.get_running_loop()
    results = await loop.run_in_executor(None, get_lemmatized_words, contents)

    word_frequencies_list = [Counter(result) for result in results]
    df = pd.DataFrame(word_frequencies_list)
    df = df.fillna(0)
    median_frequencies = df.median()
    median_frequencies = median_frequencies.sort_values(ascending=False)
    return median_frequencies


@app.get("/process-url/")
async def process_url(background_tasks: BackgroundTasks, url: str = Query(...), search_string: str = Query(...),
                      region: str = Query(...)):
    """Получает параметры запроса, сохраняет их и отправляет на обработку."""
    try:
        database = Database(DATABASE_URL)
        db_request = await database.save_request(url, search_string, region, '')
        logger.info('Request is processed and saved')
        search_results = await yandex_xmlproxy_request(search_string=search_string, region=region)
        logger.info('Yandex response is gotten')
        if search_results is not None:
            # Планируем сохранение результатов поиска в фоне
            background_tasks.add_task(database.save_search_results, db_request.id, search_results)

            # Загружаем стоп-слова из файла
            stop_words = load_stop_words("stop_words.txt")

            # Фильтруем URL-адреса по стоп-словам
            filtered_urls = filter_urls(search_results, stop_words)[:30]
            filtered_urls.append(url)
            filtered_urls = set(filtered_urls)
            logger.info('Urls are filtered')
            # Асинхронно обрабатываем все URL-адреса и сохраняем их текстовое содержимое в базе данных
            contents = await process_urls(filtered_urls)
            logger.info('Urls are processed')
            # Планируем сохранение результатов поиска в фоне
            background_tasks.add_task(database.save_page_contents, db_request.id, contents)

            main_content = contents.get(url)
            other_contents = [content for page_url, content in contents.items() if page_url != url]

            # Получаем медиану частоты встречаемости лемматизированных слов
            median_frequency = await get_median_lemmatized_word_frequency(other_contents)
            main_frequency = await get_median_lemmatized_word_frequency([main_content])
            logger.info('Frequencies of lemmas are calculated')
            # Добавляем имена к сериям
            main_frequency.name = 'main_freq'
            median_frequency.name = 'median_freq'

            # Объединяем два DataFrame по индексу
            merged_df = pd.merge(main_frequency, median_frequency, left_index=True, right_index=True, how='outer')
            logger.info('main frequency and median frequency are merged')
            # Заменяем NaN на 0
            merged_df.fillna(0, inplace=True)

            # Вычисляем разность между столбцами
            merged_df['diff'] = merged_df['median_freq'] - merged_df['main_freq']
            lsi = merged_df[(merged_df['main_freq'] == 0) & (merged_df['median_freq'] >= 1)]['median_freq']
            increase_qty = merged_df[(merged_df['main_freq'] > 0) & (merged_df['diff'] >= 10)]['diff']
            decrease_qty = merged_df[(merged_df['main_freq'] > 0) & (merged_df['diff'] <= -10)]['diff']
            logger.info('Обработка запроса завершена успешно')


        return {"status": "success",
                'lsi': lsi.to_dict() if not lsi.empty else "",
                'увеличить частотность': increase_qty.to_dict() if not increase_qty.empty else "",
                'уменьшить частотность': decrease_qty.to_dict() if not decrease_qty.empty else "",
                'обработанные ссылки': [page_url for page_url in filtered_urls if page_url != url]
                }

    except Exception as e:
        logger.error(f"Error processing request: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# @app.get("/search-google/")
# async def search_google(url: str = Query(...), search_string: str = Query(...), location: str = Query(...), domain: str = Query(...)):
#     """Получает параметры запроса, сохраняет их и отправляет на обработку."""
#     try:
#         database = Database(DATABASE_URL)
#         db_request = await database.save_request(url, search_string, location, domain)
#
#     except Exception as e:
#         logger.error(f"Error processing request: {e}")
#         raise HTTPException(status_code=500, detail="Internal server error")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host='0.0.0.0', port=5000)
