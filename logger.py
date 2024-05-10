import loguru
import sys

# Настройка логгера
logger = loguru.logger
logger.add("log/app.log", level="INFO", rotation="10 MB", compression="zip")
logger.add(sys.stderr, level="WARNING")
