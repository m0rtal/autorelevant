import pytest
import pytest_asyncio

import asyncio
from utils import *
from main import app
import uvicorn

# Пока для теста запускаю здесь
uvicorn.run(app, host='0.0.0.0', port=5000)


@pytest.mark.parametrize("search_string,region", [('Дубовые бочки', 35), ('Лист холоднокатанный', 35)])
@pytest.mark.asyncio
async def test_yandex(search_string: str, region: int):
    ''' Проверяет работу API для яндекса '''

    response = await yandex_xmlproxy_request(search_string=search_string, region=region)

    assert isinstance(response, dict)
    # проверка, собрал ли 100 страниц
    assert len(dict) == 100
    # проверка статуса ответа
    assert response['status'] == 'success'
    # проверка наличия главной метрики - lsi
    assert len(response['lsi']) > 0


@pytest.mark.parametrize("search_string,location,domain", [('Дубовые бочки', 'Krasnodar,Krasnodar Krai,Russia', 'google.ru'),
                                                           ('Лист холоднокатанный', 'Krasnodar,Krasnodar Krai,Russia', 'google.ru')])
@pytest.mark.asyncio
async def test_google(search_string: str, location: str, domain: str):
    """ Проверяет работу API для яндекса """

    response = await google_proxy_request(search_string=search_string, location=location, domain=domain)

    assert isinstance(response, dict)
    # проверка, собрал ли 100 страниц
    assert len(dict) == 100
    # проверка статуса ответа
    assert response['status'] == 'success'
    # проверка наличия главной метрики - lsi
    assert len(response['lsi']) > 0


