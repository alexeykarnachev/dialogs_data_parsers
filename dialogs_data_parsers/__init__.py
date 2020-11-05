import sys

from dialogs_data_parsers.common import log_config

sys.excepthook = log_config.handle_unhandled_exception

__version__ = '0.0.1'
