import pytest
import asyncio
import aiohttp


yandex_params = [
    {
        'url': 'https://dubovye-bochki.ru/dubovyie-bochki',
        'search_string': 'Дубовые бочки',
        'region': 35
    },
    {
        'url': 'https://asmet23.ru/list-xolodnokatanyii/',
        'search_string': 'Лист холоднокатаный',
        'region': 35
    }
]

google_params = [
    {
        'url': 'https://dubovye-bochki.ru/dubovyie-bochki',
        'search_string': 'Дубовые бочки',
        'location': 'Krasnodar, Krasnodar Krai, Russia',
        'domain': 'google.ru'
    },
    {
        'url': 'https://asmet23.ru/list-xolodnokatanyii/',
        'search_string': 'Лист холоднокатаный',
        'location': 'Krasnodar, Krasnodar Krai, Russia',
        'domain': 'google.ru'
    }
]


@pytest.fixture(params=[yandex_param for yandex_param in yandex_params])
async def yandex_response(yandex_params: dict):
    ''' Фикстура, которая обращается к API для запроса в Яндекс '''

    # обратимся к нашему API
    async with aiohttp.ClientSession() as session:
        async with session.get(url='http://localhost:5000/process-url', params=yandex_params) as response:
            response, code = await response.json(encoding='utf-8'), response.status

            return response, code


@pytest.fixture(params=[google_param for google_param in google_params])
async def google_response(google_params: dict):
    ''' Фикстура, которая обращается к API для запроса в Google '''

    # обратимся к нашему API
    async with aiohttp.ClientSession() as session:
        async with session.get(url='http://localhost:5000/search-google', params=google_params) as response:
            response, code = await response.json(encoding='utf-8'), response.status

            return response, code
