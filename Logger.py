import configparser
import traceback
import logging
import os
import datetime as dt

# Get root directory of environment
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


def log_maker(log_name, configFilePath: str = '|'):
    """
    :param configFilePath:
    :param log_name: Name of the Logs file or program that is running the Logs.
    """

    # set empty objects for error checking
    streamLogLevel = None
    fileLogLevel = None
    logger = None

    try:

        file_name = f"{dt.date.today()}-{log_name}"
        # Create base logger
        logger = logging.getLogger(file_name)

        # Create app settings parser to fetch settings
        config_parser = configparser.RawConfigParser()

        if configFilePath != '|':
            config_parser.read(configFilePath)
        else:
            config_parser.read('configs.ini')

        # Fetch the section in the app settings for logging
        log_level_stream = config_parser.get('Logging', 'log_level_stream')
        log_level_file = config_parser.get('Logging', 'log_level_file')

        # Check what the Logs level is and set the appropriate settings

        # This is for the stream that prints into the console
        if log_level_stream == "DEBUG":
            streamLogLevel = logging.DEBUG
        elif log_level_stream == "INFO":
            streamLogLevel = logging.INFO
        elif log_level_stream == "ERROR":
            streamLogLevel = logging.ERROR

        # This is for the file that saves to disk
        if log_level_file == "DEBUG":
            fileLogLevel = logging.DEBUG
        elif log_level_file == "INFO":
            fileLogLevel = logging.INFO
        elif log_level_file == "ERROR":
            fileLogLevel = logging.ERROR

        # Set the level for the console logging
        logger.setLevel(streamLogLevel)

        # Adjust the formatting for how the logs will save
        formatter = logging.Formatter('%(asctime)s : %(name)s  : %(funcName)s : %(levelname)s : %(message)s')

        # Save the Logs to the target output file and set all logging settings for the output file
        file_handler = logging.FileHandler(f"{ROOT_DIR}/Logs/{file_name}.Logs")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(fileLogLevel)

        # Set all Logs streams to the current Logs object
        logger.addHandler(file_handler)

        if config_parser.getboolean('Logging', 'log_to_console'):
            logger.addHandler(logging.StreamHandler())

    except Exception as er:
        print(f"An Error Occurred While Creating Logging Object: {er}\n{traceback.format_exc()}")

    return logger
