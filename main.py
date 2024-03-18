import asyncio
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

from fastapi import FastAPI, Request

from db_utils import add_task_to_db, get_latest_task_status_sync, get_tf_results_by_task_id
from logger_config import get_logger
from utils import process_incoming_url

logger = get_logger(__name__)
app = FastAPI()
executor = ThreadPoolExecutor()


async def run_in_threadpool(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, lambda: func(*args, **kwargs))


@app.get("/process-url")
async def process_url(request: Request):
    url = request.query_params.get('url')
    search_string = request.query_params.get('search_string')
    region = int(request.query_params.get('region'))
    if not url or not search_string or not region:
        return {"error": "URL, search_string и region должны быть указаны"}

    task_id = str(uuid4())
    logger.info(f"Получена новая задача {task_id} для URL {url}")

    # Добавляем новую задачу в БД
    await run_in_threadpool(add_task_to_db, task_id, url, search_string)

    # Обрабатываем запрос
    await process_incoming_url(task_id=task_id, url=url, search_string=search_string, region=region,
                               run_in_executor=run_in_threadpool)

    status = await run_in_threadpool(get_latest_task_status_sync, task_id)
    if status:
        if status == 'done':
            result = await run_in_threadpool(get_tf_results_by_task_id, task_id)
            return {'lsi': result.get('tf_results')}
        else:
            return {"task_id": task_id, "status": status}
    return {"message": "Статус для заданной задачи не найден"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=5000)
