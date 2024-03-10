from typing import Optional

import pandas

from logger_config import get_logger
from models import Session, Task, TaskStatus, ParsedData, Anchor, ParsedSearchData, SearchAnchor, TFIDFResult

logger = get_logger(__name__)


def add_task_to_db(task_id: str, url: str, search_string: Optional[str] = None) -> None:
    try:
        with Session() as db_session:
            new_task = Task(id=task_id, url=url, search_string=search_string)
            db_session.add(new_task)
            db_session.commit()
            logger.info(f"Task {task_id} added to DB.")

            new_status = TaskStatus(task_id=task_id, status="submitted")
            db_session.add(new_status)
            db_session.commit()
            logger.info(f"Status 'submitted' for task {task_id} added to DB.")
    except Exception as e:
        logger.error(f"Error adding task {task_id} to DB: {str(e)}")


def get_latest_task_status_sync(task_id: str) -> Optional[str]:
    try:
        with Session() as db_session:
            latest_status = db_session.query(TaskStatus).filter(TaskStatus.task_id == task_id).order_by(
                TaskStatus.timestamp.desc()).first()
            if latest_status:
                logger.info(f"Latest status for task {task_id} retrieved from DB.")
                return latest_status.status
            else:
                logger.info(f"No status found for task {task_id} in DB.")
                return None
    except Exception as e:
        logger.error(f"Error retrieving latest status for task {task_id} from DB: {str(e)}")
        return None


def add_new_status_to_db(task_id: str, status: str) -> None:
    try:
        with Session() as db_session:
            new_status = TaskStatus(task_id=task_id, status=status)
            db_session.add(new_status)
            db_session.commit()
            logger.info(f"New status '{status}' for task {task_id} added to DB.")
    except Exception as e:
        logger.error(f"Error adding new status for task {task_id} to DB: {str(e)}")


def save_parsed_data_to_db(task_id: str, parsing_result: dict) -> None:
    try:
        with Session() as db_session:
            parsed_data = ParsedData(
                task_id=task_id,
                content=parsing_result['content'],
                h1=parsing_result['h1']
            )
            db_session.add(parsed_data)
            db_session.flush()  # Это гарантирует, что parsed_data получит id после добавления в сессию, но до commit
            logger.info(f"Parsed data for task {task_id} flushed to DB.")

            # Добавляем якоря
            for anchor_text in parsing_result.get('anchors', []):
                anchor = Anchor(parsed_data_id=parsed_data.id, anchor_text=anchor_text)
                db_session.add(anchor)

            db_session.commit()
            logger.info(f"Anchors for task {task_id} added to DB.")
    except Exception as e:
        logger.error(f"Error saving parsed data for task {task_id} to DB: {str(e)}")


def save_parsed_search_data_to_db(task_id: str, parsing_result: dict, search_url: str) -> None:
    try:
        with Session() as db_session:
            parsed_search_data = ParsedSearchData(
                task_id=task_id,
                content=parsing_result['content'],
                h1=parsing_result['h1'],
                url=search_url  # Добавление нового поля url
            )
            db_session.add(parsed_search_data)
            db_session.flush()  # Это гарантирует, что parsed_search_data получит id после добавления в сессию, но до commit
            logger.info(f"Parsed data for task {task_id} flushed to DB.")

            # Добавляем якоря
            for anchor_text in parsing_result.get('anchors',
                                                  []):  # Используем get для избежания KeyError, если 'anchors' нет
                search_anchor = SearchAnchor(parsed_search_data_id=parsed_search_data.id, anchor_text=anchor_text)
                db_session.add(search_anchor)

            db_session.commit()
            logger.info(f"Anchors for task {task_id} added to DB.")
    except Exception as e:
        logger.error(f"Error saving parsed data for task {task_id} to DB: {str(e)}")


def get_content_and_anchors_by_task_id(task_id: str) -> dict:
    try:
        with Session() as db_session:
            # Получаем данные из ParsedData и соответствующие Anchor данные
            parsed_data = db_session.query(ParsedData).filter(ParsedData.task_id == task_id).first()
            parsed_data_result = {
                "content": parsed_data.content,
                "h1": parsed_data.h1,
                "anchors": [anchor.anchor_text for anchor in parsed_data.anchors]
                # предполагается наличие обратной связи в модели
            }

            # Получаем данные из ParsedSearchData и соответствующие SearchAnchor данные
            parsed_search_data = db_session.query(ParsedSearchData).filter(ParsedSearchData.task_id == task_id).all()
            parsed_search_data_result = [{
                "content": data.content,
                "h1": data.h1,
                "url": data.url,
                "anchors": [anchor.anchor_text for anchor in data.search_anchors]
                # предполагается наличие обратной связи в модели
            } for data in parsed_search_data]

            # Собираем итоговый словарь с результатами
            result = {
                "parsed_data": parsed_data_result,
                "parsed_search_data": parsed_search_data_result
            }

            logger.info(f"Data for task {task_id} successfully retrieved from DB.")
            return result
    except Exception as e:
        logger.error(f"Error retrieving data for task {task_id} from DB: {str(e)}")
        return {
            'error': str(e)
        }


def save_tfidf_results_to_db(task_id: str, tfidf_result: pandas.DataFrame) -> None:
    try:
        with Session() as db_session:
            for _, row in tfidf_result.iterrows():
                new_result = TFIDFResult(
                    task_id=task_id,
                    n_gramm=row['n-gramm'],
                    source_score=row['source_score'],
                    target_score=row['target_score'],
                    search_query=row['search_query'],
                    source_freq=row['source_freq'],
                    target_freq=row['target_freq']
                )
                db_session.add(new_result)
            db_session.commit()
            logger.info(f"TF-IDF results for task {task_id} added to DB.")
    except Exception as e:
        logger.error(f"Error saving TF-IDF results for task {task_id} to DB: {str(e)}")


def get_tfidf_results_by_task_id(task_id: str) -> dict:
    try:
        with Session() as db_session:
            # Получаем все результаты TF-IDF для данного task_id
            tfidf_results = db_session.query(TFIDFResult).filter(TFIDFResult.task_id == task_id).all()

            # Если результаты найдены, формируем список словарей с данными
            if tfidf_results:
                results_list = [{
                    'lsi': result.n_gramm,
                    'source_score': result.source_score,
                    'target_score': result.target_score,
                    'search_query': result.search_query,
                    'source_freq': result.source_freq,
                    'target_freq': result.target_freq
                } for result in tfidf_results]

                # Формируем итоговый ответ
                response = {
                    'task_id': task_id,
                    'tfidf_results': results_list
                }

                logger.info(f"TF-IDF results for task {task_id} successfully retrieved from DB.")
            else:
                # Если результатов нет, возвращаем сообщение об этом
                response = {
                    'task_id': task_id,
                    'message': 'No TF-IDF results found for this task.'
                }
                logger.info(f"No TF-IDF results found for task {task_id} in DB.")

            return response

    except Exception as e:
        logger.error(f"Error retrieving TF-IDF results for task {task_id} from DB: {str(e)}")
        return {
            'error': str(e)
        }
