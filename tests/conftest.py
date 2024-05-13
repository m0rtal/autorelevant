import pytest
import asyncio
import aiohttp


@pytest.fixture
async def yandex_response():
    ''' Фикстура, обращающаяся к API для запроса в Яндекс'''

    # Первый вид параметров, дальше будет добавлено несколько вариантов для проверки
    params = {
        'url': 'https://dubovye-bochki.ru/dubovyie-bochki',
        'search_string': 'Дубовые бочки',
        'region': 35
    }

    # обратимся к нашему API
    async with aiohttp.ClientSession() as session:
        async with session.get(url='http://localhost:5000/process-url', params=params) as response:
            response, code = await response.json(encoding='utf-8'), response.status

            return response, code


@pytest.fixture
async def google_response():
    '''Фикстура, обращающаяся к API для запроса в Google'''

    params = {
        'url': 'https://dubovye-bochki.ru/dubovyie-bochki',
        'search_string': 'Дубовые бочки',
        'location': 'Krasnodar, Krasnodar Krai, Russia',
        'domain': 'google.ru'
    }

    # обратимся к нашему API
    async with aiohttp.ClientSession() as session:
        async with session.get(url='http://localhost:5000/search-google', params=params) as response:
            response, code = await response.json(encoding='utf-8'), response.status

            return response, code
