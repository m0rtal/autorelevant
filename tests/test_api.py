import pytest
import pytest_asyncio

import asyncio


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


@pytest.mark.asyncio
async def test_google(google_response):
    ''' Проверяет работу API для яндекса '''

    response, code = await google_response

    # проверка статуса ответа
    assert code == 200
    # проверка на тип возвращаемых данных
    assert isinstance(response, dict)
    # проверка наличия главной метрики - lsi
    assert len(response['lsi']) > 0