import pytest
import pytest_asyncio

import asyncio


@pytest.mark.asyncio
async def test_yandex(yandex_response):
    ''' Проверяет работу API для яндекса '''

    response, code = await yandex_response

    assert code == 200
    assert isinstance(response, dict)
    assert len(response['lsi']) > 0
    assert 'увеличить частотность' in response
    assert 'уменьшить частотность' in response
    assert 'обработанные ссылки' in response
    assert len(response['увеличить частотность']) > 0
    assert len(response['уменьшить частотность']) > 0
    assert len(response['обработанные ссылки']) > 0


@pytest.mark.asyncio
async def test_google(google_response):
    ''' Проверяет работу API для яндекса '''

    response, code = await google_response

    assert code == 200
    assert isinstance(response, dict)
    assert len(response['lsi']) > 0
    assert 'увеличить частотность' in response
    assert 'уменьшить частотность' in response
    assert 'обработанные ссылки' in response
    assert len(response['увеличить частотность']) > 0
    assert len(response['уменьшить частотность']) > 0
    assert len(response['обработанные ссылки']) > 0