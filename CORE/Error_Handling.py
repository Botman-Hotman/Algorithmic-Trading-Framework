import inspect
import datetime as dt
from logging import Logger


class ErrorHandling:
    def __init__(self, logger: Logger):
        self.logger = logger

    # Global class variables, will be available through inheritance
    ErrorList: list = []
    ErrorsDetectedCount: int = 0
    ErrorsDetected: bool = False

    def error_details(self, message, stack_trace: str = str(0), error_code: str = str(0)):
        """
            Used to create error information in your code.
            :param stack_trace:
            :param message: Log information.
            :param error_code: The returned error code. Useful when making HTTP requests.
            :return: Error details in an array.
            """
        self.ErrorsDetectedCount = + 1
        self.logger.error(message)
        return [str(dt.datetime.now()), str(message), str(inspect.stack()[1][3]), str(self.ErrorsDetectedCount), stack_trace, error_code]

    def print_all_errors(self):
        self.logger.error([f"\n\n--- ERRORS DETECTED: {error} ---- \n\n" for error in self.ErrorList])
