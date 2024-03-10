import logging
from logging.handlers import RotatingFileHandler


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler("my_log.log", maxBytes=100000, backupCount=5)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    if not logger.handlers:  # Это предотвращает дублирование обработчиков
        logger.addHandler(handler)
    return logger
