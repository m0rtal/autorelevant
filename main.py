import sys
import os
import re

from bs4 import BeautifulSoup
from fastapi import FastAPI, Query, HTTPException
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, ForeignKey
import loguru
import aiohttp
import asyncio
import xml.etree.ElementTree as ET

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

    async def save_request(self, url: str, search_string: str, region: str):
        async with self.async_session() as session:
            try:
                async with session.begin():
                    db_request = UserRequest(url=url, search_string=search_string, region=region)
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

    async def save_page_contents(self, page_contents: list):
        async with self.async_session() as session:
            async with session.begin():
                session.add_all(page_contents)
            await session.commit()
            logger.info(f"Page contents saved: {len(page_contents)} items")


class UserRequest(Base):
    __tablename__ = "requests"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    url = Column(String)
    search_string = Column(String)
    region = Column(String)


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


async def save_page_content(url: str, request_id: int, database: Database):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                content = await response.text()
                soup = BeautifulSoup(content, "html.parser")

                # Удаление скриптов и стилей
                for script_or_style in soup(["script", "style"]):
                    script_or_style.decompose()

                # Извлекаем чистый текст из страницы с заданным разделителем
                page_content = soup.get_text(separator=" ")
                if page_content:
                    page_content = re.sub(r"\s+", " ", page_content).strip()
                    page_content = page_content.encode("utf-8", errors="replace").decode("utf-8", errors="replace")
                    await database.save_page_content(request_id, url, page_content)
    except Exception as e:
        logger.error(e)


async def process_urls(urls: list, request_id: int, database: Database):
    page_contents = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in urls:
            task = asyncio.create_task(fetch_page_content(session, url, request_id))
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        for result in results:
            if result:
                page_contents.append(result)
    if page_contents:
        await database.save_page_contents(page_contents)

async def fetch_page_content(session, url: str, request_id: int):
    try:
        async with session.get(url) as response:
            content = await response.text()
            soup = BeautifulSoup(content, "html.parser")
            for script_or_style in soup(["script", "style"]):
                script_or_style.decompose()
            page_content = soup.get_text(separator=" ").strip()
            page_content = re.sub(r"\s+", " ", page_content)
            page_content = page_content.encode("utf-8", errors="replace").decode("utf-8", errors="replace")
            return PageContent(request_id=request_id, url=url, content=page_content)
    except Exception as e:
        logger.error(f"Error fetching URL {url}: {e}")
        return None



@app.get("/process-url/")
async def process_url(url: str = Query(...), search_string: str = Query(...), region: str = Query(...)):
    """Получает параметры запроса, сохраняет их и отправляет на обработку."""
    try:
        database = Database(DATABASE_URL)
        db_request = await database.save_request(url, search_string, region)
        search_results = await yandex_xmlproxy_request(search_string=search_string, region=region)
        if search_results is not None:
            await database.save_search_results(db_request.id, search_results)
            # Загружаем стоп-слова из файла
            stop_words = load_stop_words("stop_words.txt")
            # Фильтруем URL-адреса по стоп-словам
            filtered_urls = filter_urls(search_results, stop_words)
            filtered_urls.append(url)
            filtered_urls = set(filtered_urls)
            # Асинхронно обрабатываем все URL-адреса и сохраняем их текстовое содержимое в базе данных
            await process_urls(filtered_urls, db_request.id, database)

        return {"status": "success"
                # , "data": filtered_urls
                }
    except Exception as e:
        logger.error(f"Error processing request: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host='0.0.0.0', port=5000)
