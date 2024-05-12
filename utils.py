import asyncio
import re
import ssl
from collections import Counter
from xml.etree import ElementTree as ET

import aiohttp
import pandas as pd
import tldextract
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from joblib import Parallel, delayed
from numpy import ceil
from pymystem3 import Mystem

from config import xml_user, xml_key, google_api_key, russian_stop_words
from logger import logger

mystem = Mystem()


async def process_search_results(background_tasks, database, db_request, search_results, url):
    # Планируем сохранение результатов поиска в фоне
    background_tasks.add_task(database.save_search_results, db_request.id, search_results)
    # Загружаем стоп-слова из файла
    stop_words = load_stop_words("stop_words.txt")
    # Фильтруем URL-адреса по стоп-словам
    filtered_urls = filter_urls(list(search_results.values()), stop_words)[:30]
    filtered_urls = set(filtered_urls)
    filtered_urls = {i: page_url for i, page_url in search_results.items() if page_url in filtered_urls}
    filtered_urls[0] = url
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
    merged_df = merged_df.apply(ceil).astype(int)
    # блок lsi заполнен 100 проц 10 фраз
    # < 10 фраз - блок увеличить
    # увеличить сравнивает с медианной
    # не больше 10 слов
    # if len(lsi) <= 10:
    #   add from increase qty
    #
    # if increase qty <= median:
    #   add into it

    lsi = merged_df[(merged_df['main_freq'] == 0) & (merged_df['median_freq'] > 0)]['median_freq']
    lsi = lsi.sort_values(ascending=False)
    increase_qty = merged_df[(merged_df['main_freq'] > 0) & (merged_df['diff'] > 0)]['diff']
    increase_qty = increase_qty.sort_values(ascending=False)
    decrease_qty = merged_df[(merged_df['main_freq'] > 0) & (merged_df['diff'] <= -10)]['diff']
    decrease_qty = decrease_qty.sort_values(ascending=True)

    if len(lsi) < 10 and len(increase_qty) > 10:
        need_to_add = 10 - len(lsi)
        new_lsi = pd.concat([lsi, increase_qty[:need_to_add]])
        new_increase = increase_qty[need_to_add:]
        return decrease_qty, filtered_urls, increase_qty, lsi, new_lsi, new_increase

    logger.info('Обработка запроса завершена успешно')
    return decrease_qty, filtered_urls, increase_qty, lsi, 0, 0


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
                result = await parse_xml(content)
                result = dict(enumerate(result, start=1))
                logger.info(f"Yandex XMLProxy request successful. Responce: {result}")
                return result
        except aiohttp.ClientError as e:
            logger.error(f"Yandex XMLProxy request error: {e}")
            return None


async def google_proxy_request(search_string: str, location: str, domain: str):
    url = 'https://api.spaceserp.com/google/search'

    params = {
        'apiKey': google_api_key,
        'q': search_string,
        'location': location,
        'domain': domain,
        'gl': 'ru',
        'hl': 'ru',
        'resultFormat': 'json',
        'pageSize': 100,
        'device': 'desktop'
    }

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    logger.error(f"Google SERP API request error: HTTP status code {response.status}")
                    return None

                content = await response.json()
                result = dict(enumerate([result.get('link') for result in content.get('organic_results')], start=1))
                logger.info(f"Google SERP API request successful. Responce: {result}")
                return result
        except aiohttp.ClientError as e:
            logger.error(f"Google SERP API request error: {e}")
            url = 'https://xmlstock.com/google/json/'
            lr = pd.read_csv('https://xmlstock.com/geotargets-google.csv')
            lr_value = int(lr[lr['Canonical Name'] == location]['Criteria ID'].values[0])
            params = {
                'user': xml_user,
                'key': xml_key,
                'query': search_string,
                'lr': lr_value,
                'domain': tldextract.extract(domain).suffix,
                'hl': 'ru',
                'groupby': 100,
                'device': 'desktop'
            }

            try:
                async with session.get(url, params=params) as response:
                    if response.status != 200:
                        logger.error(f"Google XMLProxy request error: HTTP status code {response.status}")
                        return None

                    content = await response.json()
                    result = dict(enumerate([result.get('url') for result in content.get('results').values()], start=1))
                    logger.info(f"Google XMLProxy request successful. Response: {result}")
                    return result
            except aiohttp.ClientError as e:
                logger.error(f"Google XMLProxy request error: {e}")
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


async def process_urls(urls: dict):
    page_contents = {}
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
        tasks = []
        for num_of_url, url in urls.items():
            task = asyncio.create_task(fetch_page_content(session, url, num_of_url))
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        for url, content in results:
            if content:
                page_contents[url] = content
    return page_contents


async def fetch_page_content(session, url: str, num_of_url: int):
    logger.info(f"Обрабатываем {num_of_url if num_of_url != 0 else 'оригинальную'} страницу {url}...")
    try:
        ua = UserAgent()
        headers = {
            'User-Agent': ua.random
        }

        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        async with session.get(url, ssl=ssl_context, headers=headers) as response:
            if response.status == 200:
                content = await response.text()
                soup = BeautifulSoup(content, "html.parser")
                for script_or_style in soup(["script", "style"]):
                    script_or_style.decompose()
                page_content = soup.get_text(separator=" ").strip()
                page_content = re.sub(r"\s+", " ", page_content)
                page_content = page_content.encode("utf-8", errors="replace").decode("utf-8", errors="replace")
                logger.info(f"Обработка страницы {url} завершена успешно")
                return (url, page_content)
            else:
                logger.warning(f"HTTP status code {response.status} for URL {url}")
                return (url, None)
    except asyncio.TimeoutError:
        logger.error(f"Timeout error fetching URL {url}")
        with open("timeout_urls.txt", "a") as f:
            f.write(url + "\n")
        return (url, None)
    except Exception as e:
        logger.error(f"Error fetching URL {url}: {e}")
        return (url, None)


def lemmatize_text(text):
    # Удаление лишнего
    text = re.sub(r"[^а-яА-ЯёЁa-zA-Z]+", " ", text)
    # Удаление цифр
    text = re.sub(r'\d+', '', text)

    lemmas = mystem.lemmatize(text.lower())

    # Получаем леммы для каждого токена в тексте, исключая стоп-слова
    lemmas = [lemma for lemma in lemmas if lemma not in russian_stop_words and lemma.strip() and len(lemma) > 2]
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
