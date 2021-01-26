import json
import logging
import multiprocessing
from pathlib import Path
import re
import unicodedata
from zipfile import BadZipFile, ZipFile

import bs4
from more_itertools import chunked

_logger = logging.getLogger(__name__)
logging.getLogger("filelock").setLevel(logging.WARNING)

DIALOG_SEPARATORS = '-‐‑‒–—―₋−⸺⸻﹘﹣－'


class FlibustaDialogsParser:
    _MIN_N_UTTERANCES = 2
    _ARCHIVE_PATTERN = re.compile(r'.*fb2-.+\.zip$')

    _DIALOGS_CHUNK_WRITE_SIZE = 1000

    def __init__(self, flibusta_archives_dir, out_file_path):
        self._flibusta_archives_dir = flibusta_archives_dir
        self._out_file_path = Path(out_file_path)
        self._out_file_path.parent.mkdir(exist_ok=True, parents=True)
        if self._out_file_path.is_file():
            self._out_file_path.unlink()

        manager = multiprocessing.Manager()
        self._archives_counter = manager.Value('i', 0)
        self._dialogs_counter = manager.Value('i', 0)
        self._out_file_lock = manager.Lock()
        self._archive_paths = list(self._iterate_on_archive_paths())

    def run(self):
        with multiprocessing.Pool() as pool:
            pool.map(self._parse_archive, self._archive_paths)

    def _iterate_on_archive_paths(self):
        for path in Path(self._flibusta_archives_dir).iterdir():
            if self._ARCHIVE_PATTERN.match(path.name):
                yield path

    def _parse_archive(self, archive_path):
        dialogs = self._iterate_on_dialogs(archive_path)

        for dialogs_chunk in chunked(dialogs, n=self._DIALOGS_CHUNK_WRITE_SIZE):
            payloads = []
            for dialog in dialogs_chunk:
                payload = json.dumps(dialog, ensure_ascii=False)
                payloads.append(payload)

            chunk_payload = '\n'.join(payloads)

            self._out_file_lock.acquire()
            with open(self._out_file_path, 'a') as out_file:
                out_file.write(chunk_payload)
                out_file.write('\n')
                out_file.flush()
            self._out_file_lock.release()

            self._dialogs_counter.value += len(dialogs_chunk)
            _logger.info(f'Archives: {self._archives_counter.value}/{len(self._archive_paths)}, '
                         f'Dialogs: {self._dialogs_counter.value}')

        self._archives_counter.value += 1

    def _iterate_on_dialogs(self, archive_path):
        book_texts = self._iterate_on_book_texts(archive_path)
        dialog_separators_set = set(DIALOG_SEPARATORS)

        for book_text in book_texts:
            book_text_lines = re.split('\n+', book_text)
            dialog = []

            for line in book_text_lines:
                line = line.strip()

                if len(line) > 2 and line[0] in dialog_separators_set:
                    line = unicodedata.normalize("NFKC", line)
                    line = re.sub(r'^\W+', '', line)
                    dialog.append(line)
                else:
                    if len(dialog) >= self._MIN_N_UTTERANCES:
                        yield dialog

                    dialog = []

            if len(dialog) >= self._MIN_N_UTTERANCES:
                yield dialog

    def _iterate_on_book_texts(self, archive_path):
        try:
            with ZipFile(archive_path, 'r') as zip_file:
                for file_name in zip_file.namelist():
                    raw_fb2_text = zip_file.read(file_name)
                    book_soup = bs4.BeautifulSoup(raw_fb2_text, features="html.parser")
                    lang_tag = book_soup.find('lang')

                    if lang_tag and lang_tag.text.lower().strip() == 'ru':
                        book_text = book_soup.text
                        yield book_text
        except BadZipFile:
            _logger.warning(f'Bad zip file: {archive_path}')
