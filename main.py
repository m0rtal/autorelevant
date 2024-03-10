import asyncio
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

from fastapi import FastAPI, BackgroundTasks

from db_utils import add_task_to_db, get_latest_task_status_sync, get_tfidf_results_by_task_id
from logger_config import get_logger
from models import SubmitURL
from utils import process_incoming_url

logger = get_logger(__name__)
app = FastAPI()
executor = ThreadPoolExecutor()


async def run_in_threadpool(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, lambda: func(*args, **kwargs))


@app.post("/submit-url", status_code=202)
async def submit_url(request: SubmitURL, background_tasks: BackgroundTasks):
    task_id = str(uuid4())
    logger.info(f"Получена новая задача {task_id} для URL {request.url}")
    await run_in_threadpool(add_task_to_db, task_id, request.url, request.search_string)
    background_tasks.add_task(process_incoming_url, request.url, task_id, run_in_threadpool)
    return {"message": "Задача принята", "task_id": task_id}


@app.get("/task/{task_id}")
async def task_status(task_id: str):
    status = await run_in_threadpool(get_latest_task_status_sync, task_id)
    if status:
        if status == 'done':
            result = await run_in_threadpool(get_tfidf_results_by_task_id, task_id)
            return {"task_id": task_id, "status": status, 'result': result.get('tfidf_results')}
        else:
            return {"task_id": task_id, "status": status}
    return {"message": "Статус для заданной задачи не найден"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=5000)
