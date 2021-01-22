import logging
import logging.config
import pathlib
import sys

from typing import Dict

_LOGGER = logging.getLogger(__name__)
_FORMATTER = '[%(asctime)s %(module)s %(funcName)s %(levelname)s] %(message)s'


def prepare_logging(logs_dir, log_files_prefix=''):
    """Configures logging."""
    log_config = _get_log_config(logs_dir, log_files_prefix)
    logging.config.dictConfig(log_config)


def handle_unhandled_exception(exc_type, exc_value, exc_traceback):
    """Handler for unhandled exceptions that will write to the logs"""
    if issubclass(exc_type, KeyboardInterrupt):
        # call the default excepthook saved at __excepthook__
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    _LOGGER.critical("Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback))


def _get_rotating_file_handler(log_file: str, level: str, max_bytes: int = 10485760, backup_count: int = 5) -> Dict:
    handler_dict = {
        'class': 'logging.handlers.RotatingFileHandler',
        'level': level,
        'formatter': 'default',
        'filename': log_file,
        'mode': 'a',
        'maxBytes': max_bytes,
        'backupCount': backup_count,
    }

    return handler_dict


def _get_console_output_handler(level) -> Dict:
    handler_dict = {
        'class': 'logging.StreamHandler',
        'level': level,
        'formatter': 'default',
    }

    return handler_dict


def _get_log_config(log_dir, log_files_prefix) -> dict:
    log_dir = pathlib.Path(log_dir)

    log_dir.mkdir(exist_ok=True, parents=True)
    info_file = str(log_dir / f'{log_files_prefix}info.log')
    errors_file = str(log_dir / f'{log_files_prefix}errors.log')
    debug_file = str(log_dir / f'{log_files_prefix}debug.log')

    handlers = {
        'info_file': _get_rotating_file_handler(info_file, 'INFO'),
        'debug_file': _get_rotating_file_handler(debug_file, 'DEBUG'),
        'errors_file': _get_rotating_file_handler(errors_file, 'ERROR'),
        'console': _get_console_output_handler('INFO')
    }

    log_config = {
        'disable_existing_loggers': False,
        'version': 1,
        'formatters': {
            'default': {
                'format': _FORMATTER
            }
        },
        'handlers': handlers,
        'loggers': {
            '': {
                'handlers': list(handlers.keys()),
                'level': 'DEBUG'
            }
        }
    }

    return log_config
