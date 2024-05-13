import pytest
import asyncio
import aiohttp

from main import app
import uvicorn
import requests

@pytest.mark.asyncio
@pytest.fixture
async def yandex_response():
    uvicorn.run(app, host='0.0.0.0', port=5000)
    post_reponse = requests.post(url='http://localhost:5000/process-url',
                                 params={
                                    'url': 'https://dubovye-bochki.ru/dubovyie-bochki/',
                                    'search_string': 'Дубовые бочки',
                                    'region': 35
                                 })

    return post_reponse