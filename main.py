import json
from io import BytesIO

import pandas as pd
from fastapi import FastAPI, Query, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse, JSONResponse

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


async def get_json(observ: pd.DataFrame,
                   ya_region: str,
                   google_region: str,
                   background_tasks: BackgroundTasks):

    url = observ["URL"].to_string(index=False)
    search_string = observ['Запрос'].to_string(index=False)

    # обращаемся к существующим API-методам напрямую
    ya_data = await process_url(
        background_tasks=background_tasks,
        url=url,
        search_string=search_string,
        region=ya_region
    )
    google_data = await search_google(
        background_tasks=background_tasks,
        url=url,
        search_string=search_string,
        location=google_region,
        domain='google.ru'
    )

    # заранее сохраним урлы, чтобы корректно соеденить ответы
    ya_urls = ya_data.pop('обработанные ссылки').items()
    google_urls = google_data.pop('обработанные ссылки').items()
    # соединяем ответы
    responses = merge_responses(ya_data, google_data)
    # формируем конечный ответ
    result = {
        'ID': observ['ID'].to_string(index=False),
        'search_string': observ['Запрос'].to_string(index=False),
        'url': observ['URL'].to_string(index=False),
        'LSI': responses['lsi'],
        'increase_qty': responses['увеличить частотность'],
        'decrease_qty': responses['уменьшить частотность'],
        'ya_urls': ya_urls,
        'google_urls': google_urls
    }

    return result


# pip install openpyxl
@app.post('/process_file/')
async def create_upload_file(background_tasks: BackgroundTasks, df: dict):
    try:
        result_df = pd.DataFrame(
            columns=['ID', 'search_string', 'url', 'LSI', 'increase_qty', 'decrease_qty', 'ya_region', 'google_region',
                     'ya_urls', 'google_urls'])

        for id in df.ID:
            observ = df[df['ID'] == id]
            result = await get_json(observ,
                            background_tasks)

            # для сохранения в Excel в виде строки
            result['LSI'] = ' '.join(result['LSI'])
            result['increase_qty'] = ' '.join(result['increase_qty'])
            result['decrease_qty'] = ' '.join(result['decrease_qty'])
            result['ya_urls'] = ' '.join(dict(result['ya_urls']).values()) # возвращает объект dict_items, поэтому заново его обьявляю dict`ом
            result['google_urls'] = ' '.join(dict(result['google_urls']).values())
            result = pd.DataFrame([result], columns=result_df.columns)

            result_df = pd.concat([result_df, result])
            logger.info(f'{id} is saved')

        buffer = BytesIO()
        with pd.ExcelWriter(buffer) as writer:
            result_df.to_excel(writer, index=False)

        logger.info('File created and returned')

        return StreamingResponse(
            BytesIO(buffer.getvalue()),
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition": f"attachment; filename=data.csv"}
        )

    except Exception as e:
        logger.error(f"Error processing request: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@app.get("/process-url/")
async def process_url(background_tasks: BackgroundTasks, url: str = Query(...), search_string: str = Query(...),
                      region: str = Query(...), return_as_json: int = 0):
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
        response = {"status": "success",
                     'lsi': [key for key in lsi.keys()] if not lsi.empty else [],
                     'увеличить частотность': increase_qty.to_dict(),
                     'уменьшить частотность': decrease_qty.to_dict(),
                     'обработанные ссылки': {i: page_url for i, page_url in filtered_urls.items() if page_url != url}
                     }
        if return_as_json:
            return json.dumps(response, indent=4, default=dict  , ensure_ascii=False).encode('utf8')

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
                        domain: str = Query(...),
                        return_as_json: int = 0,
                        ):
    """Получает параметры запроса, сохраняет их и отправляет на обработку."""
    try:
        database = Database(DATABASE_URL)
        db_request = await database.save_request(url, search_string, location, domain)
        search_results = await google_proxy_request(search_string=search_string, location=location, domain=domain)
        if search_results is not None:
            decrease_qty, filtered_urls, increase_qty, lsi = await process_search_results(background_tasks, database,
                                                                                          db_request, search_results,
                                                                                          url)
        response = {"status": "success",
                     'lsi': [key for key in lsi.keys()] if not lsi.empty else [],
                     'увеличить частотность': increase_qty.to_dict(),
                     'уменьшить частотность': decrease_qty.to_dict(),
                     'обработанные ссылки': {i: page_url for i, page_url in filtered_urls.items() if page_url != url}
                     }

        if return_as_json:
            return json.dumps(response, indent=4, default=dict, ensure_ascii=False).encode('utf8')

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


"""
объеденить данные 
превратить в два пандаса
один для яндекса другой гугл 
ток нужные поля в апиху 
в логи

"""