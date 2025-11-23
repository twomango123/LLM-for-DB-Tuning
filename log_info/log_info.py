import logging
from logging.handlers import TimedRotatingFileHandler

def get_logger():
    logger = logging.getLogger("logging")
    logger.setLevel(logging.DEBUG)  # 允许接收所有级别日志

    if logger.handlers:
        return logger  # 避免重复添加 Handler

    # 日志文件处理器：记录详细 DEBUG 信息
    file_handler = TimedRotatingFileHandler(
        "app.log",
        when="midnight",
        interval=1,
        backupCount=7,
        encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    ))

    # 控制台处理器：只显示重要信息
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # 控制台只输出 INFO+ 级别
    console_handler.setFormatter(logging.Formatter("%(message)s"))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
