import asyncio
import os
import re
import xml.etree.ElementTree as ET
from urllib.parse import urlparse

import aiohttp
import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from httpx import HTTPStatusError

from db_utils import add_new_status_to_db, save_parsed_data_to_db, save_parsed_search_data_to_db, \
    get_content_and_anchors_by_task_id, save_tf_results_to_db
from logger_config import get_logger
from tf_idf import get_tf_scores, get_all_scores

logger = get_logger(__name__)

# Загрузка переменных из .env файла
load_dotenv()

# Получение переменных и присваивание их значениям
yandex_domain = os.getenv("YANDEX_DOMAIN")
yandex_catalogue_id = os.getenv("YANDEX_CATALOGUE_ID")
yandex_filtration_type = os.getenv("YANDEX_FILTRATION_TYPE")
yandex_lng = os.getenv("YANDEX_LNG")
yandex_region = os.getenv("YANDEX_REGION")
yandex_api_key = os.getenv("YANDEX_API_KEY")


def clean_urls(urls: list, domain_part) -> list:
    stop_list = [
        'vk.com',
        'ok.ru',
        'ozon',
        'sima-land',
        'yell',
        'regtorg',
        'avito',
        'yandex',
        'youtube',
        'rutube',
        'dzen',
        'wildberries',
        'otzovik',
        't.me',
        'onlinetrade',
        'apple',
        'kupiprodai',
        'mail',
        'kino',
        'google',
        'alibaba',
        'aliexpress',
        'boxberry',
        'farpost',
        'wiki',
        'kupiprodai',
        'speedtest',
        'youla',
        'vseinstrumenti',
        'krasotaimedicina',
        'stom-firms',
        '2gis',
        'zoon',
        'infodoctor',
        'prodoctorov',
        'stomatologorg'
        'kleos',
    ]
    stop_list.append(domain_part)
    filtered_urls = [url for url in urls if not any(stop_domain in url for stop_domain in stop_list)][:20]
    return filtered_urls


def cleanup_text(content, soup):
    # Удаление скриптов и стилей
    for script in soup(["script", "style"]):
        script.decompose()

    # Очистка текста от лишних пробелов и знаков препинания
    text_parts = list(soup.stripped_strings)
    clean_text = ' '.join(text_parts)

    # Удаляем все символы, не являющиеся буквамиe
    clean_text = re.sub(r"[^а-яА-ЯёЁa-zA-Z]+", " ", clean_text)
    clean_text = re.sub(r'\s+', ' ', clean_text).strip()
    return clean_text

def parse_response_content(content: bytes, task_id: str) -> dict:
    """Функция для обработки успешного HTTP-ответа."""
    soup = BeautifulSoup(content, 'html.parser')

    # Извлекаем необходимые данные из soup
    page_content = cleanup_text(content, soup)

    h1 = soup.find('h1').get_text() if soup.find('h1') else None
    anchors_data = [' '.join(a.stripped_strings) for a in soup.find_all('a', href=True) if
                    ' '.join(a.stripped_strings).strip()]

    return {
        'task_id': task_id,
        'content': page_content,
        'h1': h1,
        'anchors': anchors_data
    }


async def fetch_and_parse(url: str, task_id: str):
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()  # Проверяем статус ответа (ошибки HTTP)

            # Проверяем, что статус ответа действительно 200
            if response.status_code == 200:
                return parse_response_content(response.content, task_id)
            else:
                logger.error(f"Непредвиденный статус ответа {response.status_code} для задачи {task_id}")
                return {
                    'task_id': task_id,
                    'error': f"Непредвиденный статус ответа: {response.status_code}"
                }

    except HTTPStatusError as http_err:
        # Обработка ошибок HTTP
        logger.error(f"HTTP ошибка при обработке задачи {task_id}: {http_err}")
        return {
            'task_id': task_id,
            'error': f"HTTP ошибка: {http_err.response.status_code}"
        }
    except Exception as e:
        # Обработка других ошибок
        logger.error(f"Ошибка при обработке задачи {task_id}: {e}")
        return {
            'task_id': task_id,
            'error': str(e)
        }


async def yandex_search(query, main_url, region=225, domain=yandex_domain, cat_id=yandex_catalogue_id,
                        filter=yandex_lng, lng=yandex_lng, api_ky=yandex_api_key):
    # Создаем URL для запроса
    url = f"https://yandex.{domain}/search/xml"
    # Параметры запроса
    params = {
        "folderid": cat_id,
        'apikey': api_ky,
        "filter": filter,
        "lr": region,
        "l10n": lng,
        'query': query,
        'maxpassages': 1,
        'page': 1,
        'groupby': 'mode=flat.groups-on-page=100.docs-in-group=1'
    }

    async with aiohttp.ClientSession() as session:
        try:
            # Отправляем GET запрос
            async with session.get(url, params=params) as response:
                logger.info(f'Ответ от яндекса: {await response.text()}')
                # Проверяем статус ответа
                if response.status == 200:
                    xml_data = await response.text()
                    # Разбор XML
                    root = ET.fromstring(xml_data)
                    urls = root.findall(".//url")
                    parsed_url = urlparse(main_url)
                    hostname = parsed_url.hostname
                    domain_part = hostname.split('.')[0]
                    filtered_urls = clean_urls([url.text for url in urls], domain_part)
                    return filtered_urls
                else:
                    logger.error(f"Ошибка запроса: {response.status}")
                    return {
                        'error': response.status
                    }
        except Exception as e:
            logger.error(f"Ошибка в выполнении запроса: {e}")


async def process_incoming_url(task_id: str, url: str, search_string: str, region: int, run_in_executor):
    parsing_result = await fetch_and_parse(url, task_id)
    if 'error' in parsing_result:
        # Если ошибка, сохраняем информацию об ошибке в БД
        await run_in_executor(add_new_status_to_db, task_id, parsing_result['error'])
    else:
        # Если ошибок нет, сохраняем собранные данные и добавляем статус "url parsed"
        await run_in_executor(save_parsed_data_to_db, task_id, parsing_result)
        await run_in_executor(add_new_status_to_db, task_id, "url parsed")
        # Ищем в яндексе
        search_results = await yandex_search(search_string, url, region)
        if 'error' in search_results:
            await run_in_executor(add_new_status_to_db, task_id, search_results['error'])
        else:
            await run_in_executor(add_new_status_to_db, task_id, "search query result received")
            logger.info(search_results)
            # Парсим результаты выдачи яндекса
            # Создаем список задач для параллельного выполнения
            tasks = [fetch_and_parse(result, task_id) for result in search_results]
            # Выполняем задачи асинхронно и получаем результаты
            results = await asyncio.gather(*tasks)
            # Обрабатываем результаты
            for result, search_url in zip(results, search_results):
                if 'error' in result:
                    await run_in_executor(add_new_status_to_db, task_id, result['error'])
                else:
                    await run_in_executor(save_parsed_search_data_to_db, task_id, result, search_url)
                    await run_in_executor(add_new_status_to_db, task_id, "search url parsed")
            # Получим данные из БД
            db_data = get_content_and_anchors_by_task_id(task_id)
            if 'error' in db_data:
                await run_in_executor(add_new_status_to_db, task_id, db_data['error'])
            else:
                tf_result = await get_tf_scores(db_data)
                await run_in_executor(add_new_status_to_db, task_id, "tf done")
                await run_in_executor(save_tf_results_to_db, task_id, tf_result)
                await run_in_executor(add_new_status_to_db, task_id, "done")


async def process_incoming_url_v2(task_id: str, url: str, search_string: str, region: int, run_in_executor):
    parsing_result = await fetch_and_parse(url, task_id)
    if 'error' in parsing_result:
        # Если ошибка, сохраняем информацию об ошибке в БД
        await run_in_executor(add_new_status_to_db, task_id, parsing_result['error'])
    else:
        # Если ошибок нет, сохраняем собранные данные и добавляем статус "url parsed"
        await run_in_executor(save_parsed_data_to_db, task_id, parsing_result)
        await run_in_executor(add_new_status_to_db, task_id, "url parsed")
        # Ищем в яндексе
        search_results = await yandex_search(search_string, url, region)
        if 'error' in search_results:
            await run_in_executor(add_new_status_to_db, task_id, search_results['error'])
        else:
            await run_in_executor(add_new_status_to_db, task_id, "search query result received")
            logger.info(search_results)
            # Парсим результаты выдачи яндекса
            # Создаем список задач для параллельного выполнения
            tasks = [fetch_and_parse(result, task_id) for result in search_results]
            # Выполняем задачи асинхронно и получаем результаты
            results = await asyncio.gather(*tasks)
            # Обрабатываем результаты
            for result, search_url in zip(results, search_results):
                if 'error' in result:
                    await run_in_executor(add_new_status_to_db, task_id, result['error'])
                else:
                    await run_in_executor(save_parsed_search_data_to_db, task_id, result, search_url)
                    await run_in_executor(add_new_status_to_db, task_id, "search url parsed")
            # Получим данные из БД
            db_data = get_content_and_anchors_by_task_id(task_id)
            if 'error' in db_data:
                await run_in_executor(add_new_status_to_db, task_id, db_data['error'])
            else:
                tf_result = await get_all_scores(db_data)
                await run_in_executor(add_new_status_to_db, task_id, "tf done")
                for result_type, df in tf_result.items():
                    await run_in_executor(save_tf_results_to_db, task_id, df, result_type)
                await run_in_executor(add_new_status_to_db, task_id, "done")