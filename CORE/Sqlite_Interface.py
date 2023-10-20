import configparser
import sqlite3
from logging import Logger

import numpy as np
import pandas as pd
from CORE.Error_Handling import ErrorHandling


class SqliteInterface(ErrorHandling):
    def __init__(self, config_object: configparser.ConfigParser, logger: Logger):
        super().__init__(logger)
        self.logger = logger
        self.config = config_object

    def excel_to_table(self, target_directory: str, db_name: str, table_name: str, idx: bool = False, index_name: str = "") -> pd.DataFrame:
        """
        :param index_name:
        :param idx:
        :param db_name: database name.
        :param target_directory: folder path for file.
        :param table_name: name to save table in SQL database.
        :return: SQL database saved to directory.
        """
        con = None
        try:

            if idx:
                excel_df = pd.read_excel(target_directory, header=1)
                excel_df.columns = excel_df.columns.str.strip()
                excel_df.set_index(index_name, inplace=True)
                self.logger.debug(f"IDX: {excel_df.index.shape} Columns: {excel_df.columns}")

            else:
                excel_df = pd.read_excel(target_directory, header=1)

            if excel_df.shape != (0, 0):
                con = sqlite3.connect(db_name, timeout=600)
                self.logger.debug(f"Adding: {table_name} to {db_name}")
                excel_df.to_sql(name=table_name, con=con, if_exists='replace')
                con.commit()
                con.close()
                self.logger.debug(f"Connection closed: {db_name}")

            else:
                self.ErrorsDetected = True
                self.ErrorList.append(self.error_details(f"{__class__}: excel_to_table -> No data to put in {table_name} in {db_name}"))

        except sqlite3.Error as error_:
            self.ErrorsDetected = True
            self.ErrorList.append(self.error_details(f"{__class__}: excel_to_table -> Failed to CREATE {table_name} in {db_name}: {error_}"))
            con.close()
            raise error_

        return excel_df

    def csv_to_table(self, target_directory: str, db_name: str, table_name: str, idx: bool = False, index_name: str = "") -> pd.DataFrame:
        """
        :param index_name:
        :param idx:
        :param db_name: database name.
        :param target_directory: folder path for file.
        :param table_name: name to save table in SQL database.
        :return: SQL database saved to directory.
        """
        con = None
        result = False
        try:

            if idx:
                csv_df = pd.read_csv(target_directory, header=1)
                csv_df.columns = csv_df.columns.str.strip()
                csv_df.set_index(index_name, inplace=True)
                self.logger.debug(f"IDX: {csv_df.index.shape} Columns: {csv_df.columns}")

            else:
                csv_df = pd.read_csv(target_directory, header=1)

            if csv_df.shape != (0, 0):
                con = sqlite3.connect(db_name, timeout=600)
                self.logger.debug(f"Adding: {table_name} to {db_name}")
                csv_df.to_sql(name=table_name, con=con, if_exists='replace')
                con.commit()
                con.close()
                self.logger.debug(f"Connection closed: {db_name}")
                result = csv_df

            else:
                self.ErrorsDetected = True
                self.ErrorList.append(self.error_details(f"{__class__}: csv_to_table -> No data to put in {table_name} in {db_name}"))

        except sqlite3.Error as error_:
            self.ErrorsDetected = True
            self.ErrorList.append(self.error_details(f"{__class__}: csv_to_table -> Failed to CREATE {table_name} in {db_name}: {error_}"))
            con.close()
            raise error_

        return result

    def df_to_table(self, db_name: str, df: pd.DataFrame or pd.Series, table_name: str):
        """
        :param db_name: database name.
        :param df: pd.DataFrame to save into SQL database.
        :param table_name: name to save table in SQL database.
        :return: SQL database saved to directory.
        """
        con = None
        try:
            con = sqlite3.connect(db_name, timeout=600)
            self.logger.debug(f"Adding: {table_name} to {db_name}")
            df.to_sql(name=table_name, con=con, if_exists='replace')
            con.commit()
            con.close()
            self.logger.debug(f"Connection closed: {db_name}")

        except sqlite3.Error as error_:
            self.ErrorsDetected = True
            self.ErrorList.append(self.error_details(f"{__class__}: df_to_table -> Failed to CREATE {table_name} in {db_name}: {error_}"))
            con.close()
            raise error_

    def table_to_df(self, db_name: str, table_name: str) -> pd.DataFrame:
        """

        :param db_name: database name.
        :param table_name: table name inside database.
        :return: table from database as a pd.DataFrame.
        """
        con = None
        df = None

        try:
            con = sqlite3.connect(db_name, timeout=600)
            self.logger.debug(f"Importing: {table_name} from {db_name} as pd.DataFrame")
            df = pd.read_sql(f"SELECT * FROM {table_name}", con=con)
            con.close()
            self.logger.debug(f"Connection closed: {db_name}")

        except sqlite3.Error as error_:
            self.ErrorsDetected = True
            self.ErrorList.append(self.error_details(f"{__class__}: table_to_df -> Failed to IMPORT {table_name} in {db_name} as pd.DataFrame: {error_}"))
            con.close()
            raise error_

        return df

    def table_to_np(self, db_name: str, table_name: str) -> np.ndarray:
        """

        :param db_name: database name.
        :param table_name: table name inside database.
        :return: table from database as a np.array.
        """
        con = None
        df = None
        try:
            con = sqlite3.connect(db_name, timeout=600)
            self.logger.debug(f"Importing: {table_name} from {db_name} as np.array")
            df = pd.read_sql(f"SELECT * FROM {table_name}", con=con)
            con.close()
            self.logger.debug(f"Connection closed: {db_name}")

        except sqlite3.Error as error_:
            self.ErrorsDetected = True
            self.ErrorList.append(self.error_details(f"{__class__}: table_to_np -> Failed to IMPORT {table_name} in {db_name} as np.array: {error_}"))
            con.close()
            raise error_

        return np.array(df)

    def update_db(self, db_name: str, df: pd.DataFrame or pd.Series, table_name: str):
        """
        :param db_name: database name.
        :param df: pd.DataFrame to update existing SQL table.
        :param table_name: name to save table in SQL database.
        :return: SQL database updated with new files.
        """
        con = None
        try:
            con = sqlite3.connect(db_name, timeout=600)
            self.logger.debug(f"Connection established:  {db_name} | {table_name}")
            df.to_sql(name=table_name, con=con, if_exists='append')
            con.commit()
            con.close()
            self.logger.debug(f"Connection closed: {db_name}")

        except sqlite3.Error as error_:
            self.ErrorsDetected = True
            self.ErrorList.append(self.error_details(f"{__class__}: update_db -> Failed to UPDATE {table_name} in {db_name}: {error_}"))
            self.logger.error(f"")
            con.close()
            raise error_

    def query_db_df(self, db_name: str, query: str) -> pd.DataFrame:
        """
        :param db_name: database name.
        :param query: an sql query in string format
        :return: an SQL query into a pd.DataFrame.
        """
        con = None
        try:
            con = sqlite3.connect(db_name, timeout=600)
            self.logger.debug(f"Querying data from {db_name} as pd.DataFrame")
            df = pd.read_sql_query(query, con)
            con.close()
            self.logger.debug(f"Connection closed: {db_name}")
            return df

        except sqlite3.Error as error_:
            self.ErrorsDetected = True
            self.ErrorList.append(self.error_details(f"{__class__}: query_db_df -> Failed to QUERY the above query as pd.DataFrame: {error_}"))
            con.close()
            raise error_

    def query_db_np(self, db_name: str, query: str) -> np.ndarray:
        """
        :param db_name: database name.
        :param query: an sql query in string format
        :return: an SQL query into a np.ndarray.
        """
        con = None
        try:
            con = sqlite3.connect(db_name, timeout=600)
            self.logger.debug(f"Querying data from {db_name} as np.array")
            df = pd.read_sql_query(query, con)
            con.close()
            self.logger.debug(f"Connection closed: {db_name}")
            return np.array(df)

        except sqlite3.Error as error_:
            self.ErrorsDetected = True
            self.ErrorList.append(self.error_details(f"{__class__}: query_db_np -> Failed to QUERY the above query as np.ndarray: {error_}"))
            con.close()
            raise error_

    def execute_query(self, db_name: str, query: str) -> None:
        con = None
        try:
            con = sqlite3.connect(db_name, timeout=600)
            self.logger.debug(f"Executing Querying {query}...")
            con.execute(query)
            con.commit()
            con.close()
            self.logger.debug(f"Connection closed: {db_name}")

        except sqlite3.Error as error_:
            self.ErrorsDetected = True
            self.ErrorList.append(self.error_details(f"{__class__}: execute_query -> Failed to EXECUTE QUERY: {error_}\n{query}"))
            con.close()
            raise error_
