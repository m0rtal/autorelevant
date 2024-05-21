from logger import logger
from datetime import datetime

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Float, select, func

Base = declarative_base()

class UserRequest(Base):
    __tablename__ = "requests"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    url = Column(String)
    search_string = Column(String)
    region = Column(String)
    domain = Column(String)
    requested_at = Column(DateTime(), default=datetime.now)


class SearchResult(Base):
    __tablename__ = "search_results"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    request_id = Column(Integer, ForeignKey("requests.id"), nullable=False)
    url = Column(String, nullable=False)
    position = Column(Integer, nullable=False)

class PageContent(Base):
    __tablename__ = "page_content"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    request_id = Column(Integer, ForeignKey("requests.id"), nullable=False)
    url = Column(String, nullable=False)
    content = Column(String, nullable=False)

class DecreaseFrequency(Base):
    __tablename__ = "decrease_frequency"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    request_id = Column(Integer, ForeignKey("requests.id"), nullable=False)
    word = Column(String, nullable=False)
    frequency_change = Column(Float, nullable=False)

class IncreaseFrequency(Base):
    __tablename__ = "increase_frequency"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    request_id = Column(Integer, ForeignKey("requests.id"), nullable=False)
    word = Column(String, nullable=False)
    frequency_change = Column(Float, nullable=False)

class LSI(Base):
    __tablename__ = "lsi"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    request_id = Column(Integer, ForeignKey("requests.id"), nullable=False)
    word = Column(String, nullable=False)


class Database:
    def __init__(self, database_url):
        self.engine = create_async_engine(database_url, echo=False, connect_args={"timeout": 15})
        self.async_session = sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)
        self.Base = Base

    async def create_all(self):
        async with self.engine.begin() as conn:
            try:
                await conn.run_sync(self.Base.metadata.create_all)
                logger.info("Tables created successfully")
            except Exception as e:
                logger.error(f"Error creating tables: {e}")
                raise e

    async def save_request(self, url: str, search_string: str, region: str, domain: str):
        async with self.async_session() as session:
            try:
                async with session.begin():
                    db_request = UserRequest(url=url, search_string=search_string, region=region, domain=domain)
                    session.add(db_request)
                await session.commit()
                logger.info(f"Request saved: {url}, {search_string}, {region}, {domain}")
                return db_request
            except Exception as e:
                logger.error(f"Error saving request {url}, {search_string}, {region}, {domain}: {e}")
                raise e

    async def save_search_results(self, request_id: int, urls: dict):
        async with self.async_session() as session:
            try:
                async with session.begin():
                    results = [SearchResult(request_id=request_id, url=url, position=i) for i, url in urls.items()]
                    session.add_all(results)
                await session.commit()
                logger.info(f"Search results saved: {len(results)} items")
            except Exception as e:
                logger.error(f"Error saving search results: {e}")
                raise e

    async def save_page_contents(self, request_id: int, contents: dict):
        async with self.async_session() as session:
            try:
                async with session.begin():
                    page_contents = [
                        PageContent(request_id=request_id, url=url, content=content)
                        for url, content in contents.items() if content is not None
                    ]
                    session.add_all(page_contents)
                await session.commit()
                logger.info(f"Page contents saved: {len(page_contents)} items")
            except Exception as e:
                logger.error(f"Error saving page contents: {e}")
                raise e

    async def save_decrease_frequency(self, request_id: int, changes: dict):
        async with self.async_session() as session:
            try:
                async with session.begin():
                    decrease_changes = [
                        DecreaseFrequency(request_id=request_id, word=word, frequency_change=freq)
                        for word, freq in changes.items()
                    ]
                    session.add_all(decrease_changes)
                await session.commit()
                logger.info(f"Decrease frequency changes saved: {len(decrease_changes)} items")
            except Exception as e:
                logger.error(f"Error saving decrease frequency changes: {e}")
                raise e

    async def save_increase_frequency(self, request_id: int, changes: dict):
        async with self.async_session() as session:
            try:
                async with session.begin():
                    increase_changes = [
                        IncreaseFrequency(request_id=request_id, word=word, frequency_change=freq)
                        for word, freq in changes.items()
                    ]
                    session.add_all(increase_changes)
                await session.commit()
                logger.info(f"Increase frequency changes saved: {len(increase_changes)} items")
            except Exception as e:
                logger.error(f"Error saving increase frequency changes: {e}")
                raise e

    async def save_lsi(self, request_id: int, words: list):
        async with self.async_session() as session:
            try:
                async with session.begin():
                    lsi_changes = [
                        LSI(request_id=request_id, word=word)
                        for word in words
                    ]
                    session.add_all(lsi_changes)
                await session.commit()
                logger.info(f"LSI words saved: {len(lsi_changes)} items")
            except Exception as e:
                logger.error(f"Error saving LSI words: {e}")
                raise e

    async def get_lsi_words(self, url_pattern: str, date_from: datetime):
        async with self.async_session() as session:
            try:
                async with session.begin():
                    # Объединяем запросы в один, используя подзапрос и join
                    stmt = (
                        select(LSI.word, func.count(LSI.word).label('count'))
                        .join(UserRequest, LSI.request_id == UserRequest.id)
                        .where(UserRequest.url.like(f"%{url_pattern}%"))
                        .where(UserRequest.requested_at >= date_from)
                        .group_by(LSI.word)
                        .order_by(func.count(LSI.word).desc())
                    )
                    result = await session.execute(stmt)
                    lsi_words = {row[0]: row[1] for row in result.fetchall()}

                    logger.info(f"LSI words retrieved successfully for URL pattern: {url_pattern}")
                    return lsi_words

            except Exception as e:
                logger.error(f"Error retrieving LSI words: {e}")
                raise e
