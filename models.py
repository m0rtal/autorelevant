from datetime import datetime

from pydantic import BaseModel

from sqlalchemy import create_engine, Column, String, Integer, ForeignKey, DateTime, Text, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

# Настройка подключения к базе данных
engine = create_engine('sqlite:///db.sqlite', echo=True)
Session = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


# Определение моделей
class Task(Base):
    __tablename__ = 'tasks'
    id = Column(String, primary_key=True)
    url = Column(String, nullable=False)
    search_string = Column(String)
    statuses = relationship("TaskStatus", back_populates="task")
    parsed_data = relationship("ParsedData", back_populates="task", uselist=False)
    tfidf_results = relationship("TFIDFResult", back_populates="task")


class TaskStatus(Base):
    __tablename__ = 'task_statuses'
    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String, ForeignKey('tasks.id'))
    status = Column(String, default="submitted")
    timestamp = Column(DateTime, default=datetime.utcnow)
    task = relationship("Task", back_populates="statuses")


class ParsedData(Base):
    __tablename__ = 'parsed_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String, ForeignKey('tasks.id'))
    content = Column(Text)
    h1 = Column(String)
    task = relationship("Task", back_populates="parsed_data")
    anchors = relationship("Anchor", back_populates="parsed_data")


class Anchor(Base):
    __tablename__ = 'anchors'
    id = Column(Integer, primary_key=True, autoincrement=True)
    parsed_data_id = Column(Integer, ForeignKey('parsed_data.id'))
    anchor_text = Column(String, nullable=False)
    parsed_data = relationship("ParsedData", back_populates="anchors")


class ParsedSearchData(Base):
    __tablename__ = 'parsed_search_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String, ForeignKey('tasks.id'))
    content = Column(Text)
    h1 = Column(String)
    url = Column(String)
    search_anchors = relationship("SearchAnchor", back_populates="parsed_search_data")


class SearchAnchor(Base):
    __tablename__ = 'search_anchors'
    id = Column(Integer, primary_key=True, autoincrement=True)
    parsed_search_data_id = Column(Integer, ForeignKey('parsed_search_data.id'))
    anchor_text = Column(String, nullable=False)
    parsed_search_data = relationship("ParsedSearchData", back_populates="search_anchors")


class TFIDFResult(Base):
    __tablename__ = 'tfidf_results'
    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String, ForeignKey('tasks.id'))
    n_gramm = Column(Text)
    source_score = Column(Float)
    target_score = Column(Float)
    search_query = Column(Text)
    source_freq = Column(Integer)
    target_freq = Column(Float)
    task = relationship("Task", back_populates="tfidf_results")


# Создание и очистка базы данных
Base.metadata.drop_all(bind=engine)
Base.metadata.create_all(bind=engine)
