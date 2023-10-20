import configparser
import contextlib
import traceback
from logging import Logger

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from CORE.Error_Handling import ErrorHandling

"""
START: pg_ctl -D pgdb -l pgdb/db_log/dblogs.log start
STOP: pg_ctl -D pgdb -l pgdb/db_log/dblogs.log stop
"""

import numpy as np
from psycopg2.extensions import register_adapter, AsIs

def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)

def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)

def addapt_numpy_float32(numpy_float32):
    return AsIs(numpy_float32)

def addapt_numpy_int32(numpy_int32):
    return AsIs(numpy_int32)

def addapt_numpy_array(numpy_array):
    return AsIs(tuple(numpy_array))

register_adapter(np.float64, addapt_numpy_float64)
register_adapter(np.int64, addapt_numpy_int64)
register_adapter(np.float32, addapt_numpy_float32)
register_adapter(np.int32, addapt_numpy_int32)
register_adapter(np.ndarray, addapt_numpy_array)


class PostgreSQLInterface(ErrorHandling):
    def __init__(self, config_object: configparser.ConfigParser, logger: Logger):

        self.logger = logger
        self.config = config_object
        super().__init__(logger)

        self.hide_progress_bar: bool = self.config.getboolean('system', 'hide_progress_bar')
        self.engine = create_engine(self.config.get('system', 'db_string'),
                                    pool_size=-1,
                                    pool_recycle=10,
                                    echo=self.config.getboolean('system', 'echo_transactions')
                                    )

    @contextlib.contextmanager
    def connect_session(self) -> Session:
        """
        Establish connection with database string found in config.
        :return:
        """
        try:
            if not self.ErrorsDetected:
                if self.engine:
                    session = Session(bind=self.engine)

                    yield session
                    self.logger.debug(f"Connected Successfully to Database")

                else:
                    self.ErrorsDetected = True
                    self.ErrorList.append(
                        self.error_details(f"{__class__}: connect_session -> Failed to create database engine"))

        except SQLAlchemyError as error_:
            self.ErrorsDetected = True
            self.ErrorList.append(
                self.error_details(f"{__class__}: connect_session -> Error connecting to the database: {error_}  {error_.code} \n {traceback.format_exc()}"))

        except Exception as error_:
            self.ErrorsDetected = True
            self.ErrorList.append(
                self.error_details(f"{__class__}: connect_session -> Error connecting to the database: {error_} \n {traceback.format_exc()}"))

        finally:
            self.close_database_connection()

    def close_database_connection(self) -> None:
        if self.engine:
            self.engine.dispose()
            self.logger.debug("Connection to the database closed.")
        else:
            self.logger.warning("No active connection to the database.")


if __name__ == '__main__':
    from CORE.Config_Manager import ConfigManager
    from Logger import log_maker

    log = log_maker('test_PostgreSQL')  # Create a log object
    config = ConfigManager(log, '../configs.ini').create_config()   # create a config object with the log object
    sql = PostgreSQLInterface(config, log) # create an sql object that has been fed by the log and config


    with sql.connect_session() as session:

        query_text = f"SELECT * FROM SCHEMA.TABLE"
        result = [row for row in session.execute(text(query_text))]
        df = pd.DataFrame(result)
