import configparser
import glob
import json
import traceback
from logging import Logger

from sqlalchemy import select
from tqdm import tqdm
from DOL.Trading.Dimensions.Dimension_Granularity import Dimension_Granularity
from DOL.Trading.Dimensions.Dimension_Instruments import Dimension_Instruments
from DOL.Trading.Dimensions.Dimension_PriceType import Dimension_PriceType
from DOL.Trading.Facts.Facts_CleanInstruments import Facts_CleanInstrument
from DOL.Trading.Facts.Facts_InstrumentsDataAligned import Facts_InstrumentsDataAligned
from CORE.Postgres_Interface import PostgreSQLInterface
from CORE.Sqlite_Interface import SqliteInterface
from CORE.Oanda_Interface import OandaInterface
from CORE.Tools import Tools
from Features.Date_Time_Factory import DateTimeFactory
from concurrent.futures import ThreadPoolExecutor

from DOL.Trading.Facts.Facts_Instruments import Facts_Instruments
import pandas as pd
import numpy as np



class Instruments(SqliteInterface, DateTimeFactory, Tools):
    def __init__(self,
                 config_object: configparser.ConfigParser,
                 postgres_interface: PostgreSQLInterface,
                 oanda_interface: OandaInterface,
                 logger: Logger,
                 granularity: str,
                 import_path_name: str):
        """
        Import / Download, clean, prepare and align all instrument data for the 'Trading' schema.
        :param config_object: ConfigManager object
        :param logger: Logger object
        :param granularity:
        """
        super().__init__(config_object, logger)
        self.logger = logger
        self.config = config_object
        self.granularity: str = granularity
        self.import_path_name = import_path_name

        # Inject other class objects for use throughout class
        self.oanda_interface = oanda_interface
        self.postgres_interface = postgres_interface

        # Generate all instrument imports from csv files found import
        self.instruments_imports: list = [i.split('/')[-1].replace('.csv', '') for i in glob.glob(f"{self.import_path_name}/*.csv")]
        self.oanda_instruments: list = self.oanda_interface.get_instruments()['name'].to_list()
        self.all_instruments: list = self.instruments_imports + self.oanda_instruments
        self.db_string_staging: str = self.config.get('system', 'db_staging_string')
        self.hide_progress_bar: bool = self.config.getboolean('system', 'hide_progress_bar')

        # Fetch label data
        with self.postgres_interface.connect_session() as session:
            granularity_stmt = select(Dimension_Granularity)
            self.granularity_dim = pd.DataFrame([row[0] for row in session.execute(granularity_stmt)])

            instrument_stmt = select(Dimension_Instruments)
            self.instrument_dim = pd.DataFrame([row[0] for row in session.execute(instrument_stmt)])

            price_stmt = select(Dimension_PriceType)
            self.price_type_dim = pd.DataFrame([row[0] for row in session.execute(price_stmt)])
            session.close()

    def populate_all_instrument_data(self) -> bool:
        """
        Concurrently run the instrument import procedures
        :return:
        """
        result = False
        try:
            if not self.ErrorsDetected:
                self.logger.debug("populate_all_instrument_data -> Starting Instrument Populater Map...")
                with ThreadPoolExecutor() as executor:
                    executor.map(self.instrument_from_imports, self.instruments_imports)
                    executor.map(self.instrument_from_oanda, self.oanda_instruments)

                # Align to the common minimum of the DateTimeKey Found in the database
                with self.postgres_interface.connect_session() as session:
                    session.bulk_save_objects(self.align_data_instruments())
                    session.commit()
                    session.close()

                if not self.ErrorsDetected:
                    result = True

            else:
                self.print_all_errors()

        except Exception as err_:
            self.ErrorsDetected = True
            self.ErrorList.append(self.error_details(f"{__class__}: populate_all_instrument_data -> {err_}\n{traceback.format_exc()}"))

        return result

    def instrument_from_oanda(self, instrument):
        """
        This will import the target instrument from oanda concurrently in the required chunks 
        :param instrument: str name of instrument on OANDA XXX_XXX
        """
        result = False
        if not self.ErrorsDetected:
            if not instrument.isspace():
                self.logger.debug(f"instrument_from_oanda -> Importing Data for {instrument}...")

                oanda_instrument_data = self.oanda_interface.get_historical_candles(instrument, 'M')

                if oanda_instrument_data.shape[0] > 0:
                    # Prepare the data for entry into the database
                    fact_instrument = self.prepare_data_instrument(oanda_instrument_data,
                                                                   instrument)

                    fact_clean_instrument = self.fillna_data_instruments(oanda_instrument_data,
                                                                         instrument,
                                                                         self.config.get('system', 'fill_missing_values'))

                    # Check that buckets off data have information
                    if fact_instrument is not False and fact_clean_instrument is not False:
                        with self.postgres_interface.connect_session() as session:

                            session.bulk_save_objects(fact_instrument)
                            session.bulk_save_objects(fact_clean_instrument)
                            session.commit()
                            session.close()

                        result = True

                    else:
                        self.ErrorsDetected = True
                        self.ErrorList.append(self.error_details(
                            f"{__class__}: instrument_from_imports -> List of Facts Objects is empty or with error for {instrument}"))

                else:
                    self.ErrorsDetected = True
                    self.ErrorList.append(self.error_details(
                        f"{__class__}: instrument_from_oanda -> Oanda DataFrame Empty For {instrument}"))

            else:
                self.logger.warning('instrument_from_oanda -> Instrument name string is blank, passing ...')
                pass
        else:
            self.print_all_errors()

        return result

    def instrument_from_imports(self, instrument: str) -> bool:
        """
        Any OHLCV data that is in .csv files will be imported as the file name.
        :param instrument: str name of file
        :return: True of successful
        """
        result: bool = False
        if not self.ErrorsDetected:
            if not instrument.isspace():
                self.logger.debug(f"instrument_from_imports -> Importing local files for {instrument}...")

                # check Imports Folder for CSV's of Instruments
                # import into imported instrument object
                raw_import_instrument = self.csv_to_table(f"{self.import_path_name}/{instrument}.csv",
                                                          self.db_string_staging,
                                                          f"{instrument}_Raw", True, 'Date')

                if raw_import_instrument.shape[0] > 0:
                    # Prepare the data for entry into the database
                    fact_instrument = self.prepare_data_instrument(raw_import_instrument,
                                                                   instrument)

                    # Create clean version with no NA values
                    fact_clean_instrument = self.fillna_data_instruments(raw_import_instrument,
                                                                         instrument,
                                                                         self.config.get('system', 'fill_missing_values'))

                    # Check that buckets off data have information
                    if fact_instrument is not False and fact_clean_instrument is not False:
                        with self.postgres_interface.connect_session() as session:

                            session.bulk_save_objects(fact_instrument)
                            session.bulk_save_objects(fact_clean_instrument)

                            session.commit()
                            session.close()
                            result = True

                    else:
                        self.ErrorsDetected = True
                        self.ErrorList.append(self.error_details(
                            f"{__class__}: instrument_from_imports -> List of Facts Objects is empty or with error for {instrument}"))

                else:
                    self.ErrorsDetected = True
                    self.ErrorList.append(self.error_details(
                        f"{__class__}: instrument_from_imports -> Raw Data Imported is Null for {instrument}"))

            else:
                self.logger.warning('instrument_from_imports -> Instrument name string is blank, passing ...')
                pass

        else:
            self.print_all_errors()

        return result

    def prepare_data_instrument(self, df: pd.DataFrame, instrument_name: str, price_type: str = 'M') -> list:
        """
        Create Date Time keys, Granularity Key and Instrument Keys
        :param df: A pd.DataFrame of OHLCV data with datetime index
        :param instrument_name: Name of the target instrument
        :param price_type: Price Type Bid, Ask, Mid
        :return: list[Facts_Instruments]
        """
        self.logger.debug(f"Starting Instrument data prep for {instrument_name} ...")
        result = False
        if not self.ErrorsDetected:
            if df.shape[0] > 0:

                # Generate date time integers
                ints_dt = self.datetime_to_int(df.index.values)

                dff = pd.DataFrame(df)
                dff.index = ints_dt
                dff.index.rename('DateTimeKey', inplace=True)

                # Check incoming date time index for granularity type
                GranularityName = self.granularity_check(ints_dt,
                                                         self.config.getint('system',
                                                                            'granularity_sample_amount'))

                if GranularityName == self.granularity:
                    dff['GranularityKey'] = self.granularity_dim.query(f"OandaAlias == '{GranularityName}'")['GranularityKey'].values[0]
                    dff['InstrumentKey'] = self.instrument_dim.query(f"Name == '{instrument_name}'")['InstrumentKey'].values[0]
                    dff['PriceTypeKey'] = self.price_type_dim.query(f"Alias == '{price_type}'")['PriceTypeKey'].values[0]
                    dff['DateKey'] = self.date_key_parser(dff.index.values)
                    dff['TimeKey'] = self.time_key_parser(dff.index.values)
                    dff = json.loads(dff.reset_index().to_json(orient='records'))

                    result = [Facts_Instruments(
                        id=self.create_hex_from_string(str(row)),
                        DateTimeKey=row['DateTimeKey'],
                        DateKey=row['DateKey'],
                        TimeKey=row['TimeKey'],
                        GranularityKey=row['GranularityKey'],
                        PriceTypeKey=row['PriceTypeKey'],
                        Open=row['Open'],
                        High=row['High'],
                        Low=row['Low'],
                        Close=row['Close'],
                        Volume=row['Volume'],
                        InstrumentKey=row['InstrumentKey']

                    ) for idx, row
                        in tqdm(enumerate(dff),
                                total=len(dff),
                                desc=f"{instrument_name}_{price_type} PREPARATION",
                                ascii=True,
                                disable=self.hide_progress_bar)]

                else:
                    self.ErrorList.append(self.error_details(
                        f"{__class__}: prepare_data_instrument -> {instrument_name} Granularity '{GranularityName}' Does not match target '{self.granularity}'"))
                    pass

            else:
                self.ErrorsDetected = True
                self.ErrorList.append(self.error_details(
                    f"{__class__}: prepare_data_instrument -> {instrument_name} No Data to prepare"))

        else:
            self.print_all_errors()

        return result

    def fillna_data_instruments(self, df: pd.DataFrame, instrument_name: str, nulls_method: str = '|', price_type: str = 'M') -> list:
        """
        fill all NA values
        :param df:
        :param instrument_name:
        :param nulls_method: interpolate_lin, interpolate_poly
        :param price_type: Price Type Bid, Ask, Mid
        :return: List of instruments with a complete date time range with missing values solved through target method
        """
        self.logger.debug(f"Starting Instrument Fill NA for {instrument_name} {nulls_method}...")
        result = False
        if not self.ErrorsDetected:
            if df.shape[0] > 0:
                ints_dt = self.datetime_to_int(df.index.values)

                dff: pd.DataFrame = df.copy()
                dff.index = ints_dt
                dff.index.rename('DateTimeKey', inplace=True)

                # Check incoming date time index for granularity type
                GranularityName = self.granularity_check(ints_dt,
                                                         self.config.getint('system',
                                                                            'granularity_sample_amount'))
                if GranularityName == self.granularity:
                    # Create Date Time index with full range of dates
                    dates = self.datetime_to_int(pd.date_range(start=df.index.values[0],
                                                               end=df.index.values[-1],
                                                               freq=GranularityName))
                    dfx = pd.DataFrame(index=dates)
                    dfx.index.rename('DateTimeKey', inplace=True)

                    # outer join the full range of dates it will generate the missing value date time
                    # for the target data
                    dff = pd.merge(dfx, dff, on='DateTimeKey', how='outer').sort_index()

                    # Get count of None values in data frame
                    none_values: int = dff.isna().sum().sum()

                    if none_values > 0 and nulls_method == 'interpolate_lin':
                        self.logger.debug(f"fillna_data_instruments -> data has {none_values} missing values, linear-fill")
                        dff.interpolate(method='linear', inplace=True)

                    elif none_values > 0 and nulls_method == 'interpolate_poly':
                        self.logger.debug(f"fillna_data_instruments -> data has {none_values} missing values, poly-fill")
                        dff.interpolate(method='polynomial', inplace=True,
                                        order=self.config.getint('system', 'null_polynomial_order'))

                    dff['GranularityKey'] = self.granularity_dim.query(f"OandaAlias == '{GranularityName}'")['GranularityKey'].values[0]
                    dff['InstrumentKey'] = self.instrument_dim.query(f"Name == '{instrument_name}'")['InstrumentKey'].values[0]
                    dff['PriceTypeKey'] = self.price_type_dim.query(f"Alias == '{price_type}'")['PriceTypeKey'].values[0]
                    dff['DateKey'] = self.date_key_parser(dff.index.values)
                    dff['TimeKey'] = self.time_key_parser(dff.index.values)
                    dff.dropna(inplace=True)
                    dff = json.loads(dff.reset_index().to_json(orient='records'))

                    result = [Facts_CleanInstrument(
                        id=self.create_hex_from_string(str(row)),
                        DateTimeKey=row['DateTimeKey'],
                        DateKey=row['DateKey'],
                        TimeKey=row['TimeKey'],
                        GranularityKey=row['GranularityKey'],
                        PriceTypeKey=row['PriceTypeKey'],
                        Open=row['Open'],
                        High=row['High'],
                        Low=row['Low'],
                        Close=row['Close'],
                        Volume=row['Volume'],
                        InstrumentKey=row['InstrumentKey']

                    ) for idx, row in
                        tqdm(enumerate(dff),
                             total=len(dff),
                             desc=f"{instrument_name}_{price_type} CLEAN",
                             ascii=True,
                             disable=self.hide_progress_bar)]

                else:
                    self.ErrorList.append(self.error_details(
                        f"{__class__}: fillna_data_instruments -> {instrument_name} Granularity '{GranularityName}' Does not match target '{self.granularity}'"))
                    pass

            else:
                self.ErrorsDetected = True
                self.ErrorList.append(self.error_details(
                    f"{__class__}: fillna_data_instruments -> {instrument_name} {nulls_method} No Data to clean"))

        else:
            self.print_all_errors()

        return result

    def align_data_instruments(self) -> list:
        """
        Input a Facts_Instruments total, it will align the data to the lowest date time of all items.
        Is useful in machine learning
        :return: List[Facts_InstrumentsDataAligned]
        """
        self.logger.debug(f"Starting Instrument data alignment ...")
        dff: pd.DataFrame = pd.DataFrame()
        result = False

        if not self.ErrorsDetected:

            # Pull entire fact instruments table to create an aligned instruments table
            with self.postgres_interface.connect_session() as session:
                instrument_stmt = select(Facts_Instruments)
                data_to_clean = pd.DataFrame([row[0] for row in session.execute(instrument_stmt)]).set_index('DateTimeKey').sort_index()

            if data_to_clean.shape != (0, len(data_to_clean.columns)):
                align_index_to = max([min(data_to_clean.query(f"InstrumentKey == {i}").index) for i in data_to_clean['InstrumentKey'].unique()])
                data_to_clean.sort_index(inplace=True)

                # use this boolean index to filter the other arrays
                arr = np.array(data_to_clean.index, dtype=int) >= int(align_index_to)
                dff['DateTimeKey'] = data_to_clean.index.values[arr]
                dff['DateKey'] = data_to_clean['DateKey'].values[arr]

                dff['TimeKey'] = data_to_clean['TimeKey'].values[arr]
                dff['GranularityKey'] = data_to_clean['GranularityKey'].values[arr]
                dff['Open'] = data_to_clean['Open'].values[arr]
                dff['High'] = data_to_clean['High'].values[arr]
                dff['Low'] = data_to_clean['Low'].values[arr]
                dff['Close'] = data_to_clean['Close'].values[arr]
                dff['Volume'] = data_to_clean['Volume'].values[arr]
                dff['InstrumentKey'] = data_to_clean['InstrumentKey'].values[arr]
                dff['PriceTypeKey'] = data_to_clean['PriceTypeKey'].values[arr]
                dff = json.loads(dff.reset_index().to_json(orient='records'))

                result = [Facts_InstrumentsDataAligned(
                    id=self.create_hex_from_string(str(row)),
                    DateTimeKey=row['DateTimeKey'],
                    DateKey=row['DateKey'],
                    TimeKey=row['TimeKey'],
                    GranularityKey=row['GranularityKey'],
                    PriceTypeKey=row['PriceTypeKey'],
                    Open=row['Open'],
                    High=row['High'],
                    Low=row['Low'],
                    Close=row['Close'],
                    Volume=row['Volume'],
                    InstrumentKey=row['InstrumentKey']

                ) for idx, row
                    in tqdm(enumerate(dff),
                            total=len(dff),
                            desc=f"ALIGNING DATA",
                            ascii=True,
                            disable=self.hide_progress_bar)]

            else:
                self.ErrorsDetected = True
                self.ErrorList.append(self.error_details(f"{__class__}: align_data_instruments -> No Data in data_to_clean"))

        else:
            self.print_all_errors()

        return result


if __name__ == '__main__':
    from CORE.Config_Manager import ConfigManager
    from Logger import log_maker

    logs = log_maker('InitialiseDataBase', '../../configs.ini')
    cm = ConfigManager(logs, '../../configs.ini')
    sql = PostgreSQLInterface(cm.create_config(), logs)
    oanda = OandaInterface(cm.create_config(), logs, 'D')
    Instruments(cm.create_config(),
                sql,
                oanda,
                logs,
                'D',
                '../../Imports').instrument_from_oanda('EUR_GBP')
