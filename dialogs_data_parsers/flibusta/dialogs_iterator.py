import json
import logging

_logger = logging.getLogger(__name__)


class FlibustaDialogsIterator:
    def __init__(self, file_path, logging_period):
        self._file_path = file_path
        self._logging_period = logging_period

    def __iter__(self):
        with open(self._file_path) as file:
            n_samples_done = 0
            for n_lines_done, raw_line in enumerate(file, start=1):
                if self._logging_period and n_lines_done % self._logging_period:
                    _logger.info(f'Flibusta lines: {n_lines_done}, samples: {n_samples_done}')

                dialog = json.loads(raw_line)

                for n_utterances in range(2, len(dialog) + 1):
                    subdialog = dialog[:n_utterances]
                    n_samples_done += 1
                    yield subdialog
