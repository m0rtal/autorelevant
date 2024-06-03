import asyncio
from io import BytesIO

import aiohttp
import pandas as pd
from fastapi import FastAPI, Query, HTTPException, BackgroundTasks, File, UploadFile, Request

from config import DATABASE_URL
from db_utils import Database
from logger import logger
from utils import process_search_results, yandex_xmlproxy_request, google_proxy_request, merge_responses

from datetime import datetime, timedelta


async def startup():
    # Создание таблиц, если они еще не созданы
    database = Database(DATABASE_URL)
    await database.create_all()


# FastAPI app
app = FastAPI()
app.add_event_handler("startup", startup)


# pip install openpyxl
async def get_json(result_df: pd.DataFrame,
                   observ: pd.Series,
                   background_tasks):
        ya_params = {
            'url': observ["URL"].to_string(index=False),
            'search_string': observ['Запрос'].to_string(index=False),
            'region': observ['yandex_region'].to_string(index=False)
        }

        google_params = {
            'url': observ["URL"].to_string(index=False),
            'search_string': observ['Запрос'].to_string(index=False),
            'location': observ['google_region'].to_string(index=False),
            'domain': 'google.ru'
        }



        try:
            async with aiohttp.ClientSession() as session:
                async with session.get('http://0.0.0.0:5000/process-url/', params=ya_params) as response:
                    ya_data = await response.json() if response.status == 200 else None

            async with aiohttp.ClientSession() as session:
                async with session.get('http://0.0.0.0:5000/search-google', params=google_params) as response:
                    google_data = await response.json() if response.status == 200 else None

        except aiohttp.ClientError as e:
            logger.error(f"Google XMLProxy request error: {e}")
            return None

        if ya_data is None or google_data is None:
            return None

        ya_urls = ya_data.pop('обработанные ссылки').items()
        google_urls = google_data.pop('обработанные ссылки').items()
        google_data.__delitem__('status')
        ya_data.__delitem__('status')

        result = merge_responses(ya_data, google_data)

        print('test')
        return None


@app.post('/process_file/')
async def create_upload_file(background_tasks: BackgroundTasks, file: UploadFile):
    content = await file.read()
    df = pd.read_excel(BytesIO(content))
    result_df = pd.DataFrame(columns=['ID', 'LSI', 'search_string', 'url', 'region', 'increase_qty', 'decrease_qty'])
    tasks = []

    # for id in df.ID:
    yandex_tasks = []
    google_tasks = []

    for id in df.ID:
        observ = df[df['ID'] == id]
        ya_task = asyncio.create_task(get_json(result_df,
                                               observ,
                                               background_tasks))
        yandex_tasks.append(ya_task)
        # google_tasks.append(google_task)

    ya_result = asyncio.gather(yandex_tasks)
    google_result = asyncio.gather(google_tasks)

    return {'content': df}


@app.get("/process-url/")
async def process_url(background_tasks: BackgroundTasks, url: str = Query(...), search_string: str = Query(...),
                      region: str = Query(...)):
    """Получает параметры запроса, сохраняет их и отправляет на обработку."""
    try:
        database = Database(DATABASE_URL)
        db_request = await database.save_request(url, search_string, region, '')
        search_results = await yandex_xmlproxy_request(search_string=search_string, region=region)
        if search_results is not None:
            decrease_qty, filtered_urls, increase_qty, lsi = await process_search_results(background_tasks, database,
                                                                                          db_request, search_results,
                                                                                          url)
        # background_tasks.add_task(database.save_results, db_request, decrease_qty, increase_qty, lsi)

        return {"status": "success",
                'lsi': [key for key in lsi.keys()] if not lsi.empty else [],
                'увеличить частотность': [f"{key}: {value}" for key, value in
                                          increase_qty.items()] if not increase_qty.empty else [],
                'уменьшить частотность': [f"{key}: {value}" for key, value in
                                          decrease_qty.items()] if not decrease_qty.empty else [],
                'обработанные ссылки': {i: page_url for i, page_url in filtered_urls.items() if page_url != url}
                }

    except Exception as e:
        logger.error(f"Error processing request: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@app.get("/search-google/")
async def search_google(background_tasks: BackgroundTasks, url: str = Query(...), search_string: str = Query(...),
                        location: str = Query(...),
                        domain: str = Query(...)):
    """Получает параметры запроса, сохраняет их и отправляет на обработку."""
    try:
        database = Database(DATABASE_URL)
        db_request = await database.save_request(url, search_string, location, domain)
        search_results = await google_proxy_request(search_string=search_string, location=location, domain=domain)
        if search_results is not None:
            decrease_qty, filtered_urls, increase_qty, lsi = await process_search_results(background_tasks, database,
                                                                                          db_request, search_results,
                                                                                          url)

        # background_tasks.add_task(database.save_results, db_request, decrease_qty, increase_qty, lsi)

        return {"status": "success",
                'lsi': [key for key in lsi.keys()] if not lsi.empty else [],
                'увеличить частотность': [f"{key}: {value}" for key, value in
                                          increase_qty.items()] if not increase_qty.empty else [],
                'уменьшить частотность': [f"{key}: {value}" for key, value in
                                          decrease_qty.items()] if not decrease_qty.empty else [],
                'обработанные ссылки': {i: page_url for i, page_url in filtered_urls.items() if page_url != url}
                }

    except Exception as e:
        logger.error(f"Error processing request: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/result-lsi/")
async def result_lsi(url: str = Query(...)):
    """Возвращает все слова lsi с числом встречаемости для запросов, где URL похож на передаваемый."""
    try:
        database = Database(DATABASE_URL)
        # Определяем дату 30 дней назад
        date_30_days_ago = datetime.now() - timedelta(days=30)
        lsi_words = await database.get_lsi_words(url, date_30_days_ago)

        return {"status": "success", "lsi_words": lsi_words}

    except Exception as e:
        logger.error(f"Error processing LSI words request: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host='0.0.0.0', port=5000)
