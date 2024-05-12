from fastapi import FastAPI, Query, HTTPException, BackgroundTasks

from config import DATABASE_URL
from db_utils import Database
from logger import logger
from utils import process_search_results, yandex_xmlproxy_request, google_proxy_request


async def startup():
    # Создание таблиц, если они еще не созданы
    database = Database(DATABASE_URL)
    await database.create_all()


# FastAPI app
app = FastAPI()
app.add_event_handler("startup", startup)


@app.get("/process-url/")
async def process_url(background_tasks: BackgroundTasks, url: str = Query(...), search_string: str = Query(...),
                      region: str = Query(...)):
    """Получает параметры запроса, сохраняет их и отправляет на обработку."""
    try:
        database = Database(DATABASE_URL)
        db_request = await database.save_request(url, search_string, region, '')
        search_results = await yandex_xmlproxy_request(search_string=search_string, region=region)
        if search_results is not None:
            decrease_qty, filtered_urls, increase_qty, lsi, new_lsi, new_increase = await process_search_results(background_tasks, database,
                                                                                          db_request, search_results,
                                                                                          url)

        return {"status": "success",
                'lsi': [key for key in lsi.keys()] if not lsi.empty else [],
                'увеличить частотность': [f"{key}: {value}" for key, value in
                                          increase_qty.items()] if not increase_qty.empty else [],
                'уменьшить частотность': [f"{key}: {value}" for key, value in
                                          decrease_qty.items()] if not decrease_qty.empty else [],
                'обработанные ссылки': {i: page_url for i, page_url in filtered_urls.items() if page_url != url},
                'new_lsi': new_lsi,
                'new_increase': new_increase
                }

    except Exception as e:
        logger.error(f"Error processing request: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@app.get("/search-google/")
async def search_google(background_tasks: BackgroundTasks, url: str = Query(...), search_string: str = Query(...), location: str = Query(...),
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host='0.0.0.0', port=5000)
