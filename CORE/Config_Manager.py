import configparser
from logging import Logger
from CORE.Error_Handling import ErrorHandling


class ConfigManager(ErrorHandling):
    def __init__(self, logger: Logger, config_path: str):
        super().__init__(logger)
        self.logger = logger
        self.config_path: str = config_path
        self.config: configparser.ConfigParser

    def create_config(self) -> configparser.ConfigParser:
        """
        creates config object
        :return: ConfigParser object
        """
        self.logger.debug(f"Fetching Config Data For '{self.config_path}'...")

        config: configparser.ConfigParser = configparser.ConfigParser()
        try:
            config.read(self.config_path)

            if config is not None:
                self.logger.debug('Fetching Config Data Successful')

            else:
                self.ErrorsDetected = True
                self.ErrorList.append(
                    self.error_details(f"{__class__}: create_config -> Config File '{self.config_path}' Returned None."))

        except FileNotFoundError:
            self.ErrorsDetected = True
            self.ErrorList.append(
                self.error_details(f"{__class__}: create_config -> Config file '{self.config_path}' not found"))

        except configparser.Error as e:
            self.ErrorsDetected = True
            self.ErrorList.append(self.error_details(
                f"{__class__}: create_config -> Failed to read the config file '{self.config_path}' {str(e)}"))

        except Exception as error_:
            self.ErrorsDetected = True
            self.ErrorList.append(self.error_details(f"{__class__}: create_config -> An Error Has Occurred {error_}"))

        return config

    def update_config(self, section, option, value):
        """
        Update config files
        :param section:
        :param option:
        :param value:
        :return:
        """
        self.logger.debug(f"Updating Config Data For '{self.config_path}' {section} | {option} | {value}...")
        try:
            if not self.ErrorsDetected:
                if self.config is not None:
                    self.config.set(section, option, value)
                    with open(self.config_path, 'w') as config_file:
                        self.config.write(config_file)
                    self.logger.debug("Configuration file updated successfully.")

                else:
                    self.ErrorsDetected = True
                    self.ErrorList.append(self.error_details(
                        f"{__class__}: update_config -> Config Object For '{self.config_path}' Returned None."))

        except (configparser.Error, FileNotFoundError) as e:
            self.ErrorsDetected = True
            print(f"Error: Failed to update the config file. Reason: {str(e)}")

        except Exception as error_:
            self.ErrorsDetected = True
            self.ErrorList.append(self.error_details(f"{__class__}: _create_config -> An Error Has Occurred {error_}"))
