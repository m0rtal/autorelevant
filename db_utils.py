from logger import logger
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, ForeignKey

Base = declarative_base()

class UserRequest(Base):
    __tablename__ = "requests"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    url = Column(String)
    search_string = Column(String)
    region = Column(String)
    domain = Column(String)


class SearchResult(Base):
    __tablename__ = "search_results"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    request_id = Column(Integer, ForeignKey("requests.id"), nullable=False)
    url = Column(String, nullable=False)


class PageContent(Base):
    __tablename__ = "page_content"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    request_id = Column(Integer, ForeignKey("requests.id"), nullable=False)
    url = Column(String, nullable=False)
    content = Column(String, nullable=False)


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

    async def save_search_results(self, request_id: int, urls: list):
        async with self.async_session() as session:
            try:
                async with session.begin():
                    results = [SearchResult(request_id=request_id, url=url) for url in urls]
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
