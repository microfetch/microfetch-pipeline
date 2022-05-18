import logging
import sqlalchemy
from sqlalchemy.orm import Session

from .database import get_engine, Tables


# Adapted from https://stackoverflow.com/a/67305494
class DatabaseHandler(logging.Handler):
    backup_logger = None

    def __init__(self, level=0, backup_logger_name=None):
        super().__init__(level)
        if backup_logger_name:
            self.backup_logger = logging.getLogger(backup_logger_name)

    def emit(self, record):
        try:
            msg = record.msg.replace('\n', '\t').replace("'", "''")
            with Session(get_engine()) as session:
                session.execute(sqlalchemy.text((
                    f"INSERT INTO {Tables.LOGGING.value} (msg, level, logger_name, trace, create_datetime) "
                    f"VALUES ('{msg}', {record.levelno}, '{record.name}', '{record.stack_info}', now())"
                )))
                session.commit()
        except:
            pass


log_format = logging.Formatter(fmt='%(asctime)s %(levelname)s:\t%(message)s')
stream_handler = logging.StreamHandler()
