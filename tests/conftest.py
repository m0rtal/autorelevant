import pytest
import asyncio
import aiohttp


@pytest.fixture
async def yandex_response():
    params = {
        'url': 'https://dubovye-bochki.ru/dubovyie-bochki',
        'search_string': 'Дубовые бочки',
        'region': 35
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url='http://localhost:5000/process-url', params=params) as response:
            response, code = await response.json(encoding='utf-8'), response.status

            return response, code

