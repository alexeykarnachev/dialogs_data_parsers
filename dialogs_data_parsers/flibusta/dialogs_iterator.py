import json

from tqdm import tqdm


class FlibustaDialogsIterator:
    def __init__(self, file_path, min_n_messages_in_dialog=1, verbose=True):
        self._file_path = file_path
        self._min_n_messages_in_dialog = min_n_messages_in_dialog
        self._verbose = verbose

    def __iter__(self):
        with open(self._file_path) as file:
            file = tqdm(file, desc='Flibusta lines done') if self._verbose else file
            for raw_line in file:
                dialog = json.loads(raw_line)
                if len(dialog) >= self._min_n_messages_in_dialog:
                    yield dialog
