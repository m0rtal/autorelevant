import pytest
import pytest_asyncio

import asyncio
from utils import *


# Пока для теста запускаю здесь


@pytest.mark.asyncio
async def test_yandex(yandex_response):
    ''' Проверяет работу API для яндекса '''

    response, code = await yandex_response

    # проверка статуса ответа
    assert code == 200
    # проверка на тип возвращаемых данных
    assert isinstance(response, dict)
    # проверка наличия главной метрики - lsi
    assert len(response['lsi']) > 0





# @pytest.mark.parametrize("search_string,location,domain", [('Дубовые бочки', 'Krasnodar,Krasnodar Krai,Russia', 'google.ru'),
#                                                            ('Лист холоднокатанный', 'Krasnodar,Krasnodar Krai,Russia', 'google.ru')])
# @pytest.mark.asyncio
# async def test_google(search_string: str, location: str, domain: str):
#     """ Проверяет работу API для яндекса """
#
#     response = await google_proxy_request(search_string=search_string, location=location, domain=domain)
#
#     assert isinstance(response, dict)
#     # проверка, собрал ли 100 страниц
#     assert len(dict) == 100
#     # проверка статуса ответа
#     assert response['status'] == 'success'
#     # проверка наличия главной метрики - lsi
#     assert len(response['lsi']) > 0


