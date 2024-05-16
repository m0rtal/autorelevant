import pytest
import asyncio
import aiohttp
from tests.params import yandex_params, google_params


async def fetch_response(url, params):
    async with aiohttp.ClientSession() as session:
        async with session.get(url=url, params=params) as response:
            return await response.json(encoding='utf-8'), response.status


@pytest.fixture(params=yandex_params)
async def yandex_response(request):
    return await fetch_response('http://localhost:5000/process-url', request.param)


@pytest.fixture(params=google_params)
async def google_response(request):
    return await fetch_response('http://localhost:5000/search-google', request.param)
