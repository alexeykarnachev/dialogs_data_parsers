import json
import logging
import re
import unicodedata
from itertools import islice, chain
from pathlib import Path
from zipfile import ZipFile, BadZipFile

import bs4
from more_itertools import chunked
from tqdm import tqdm

_logger = logging.getLogger(__name__)


class FlibustaDialogsParser:
    _MIN_N_UTTERANCES = 2
    _ARCHIVE_PATTERN = re.compile('.+fb2-.+\.zip')
    _DIALOG_SEPARATORS = '-‐‑‒–—―₋−⸺⸻﹘﹣－'
    _AUTHOR_WORDS_SEPARATOR_PATTERN = re.compile(f'([.,!?]) +[{_DIALOG_SEPARATORS}]')

    def __init__(self, flibusta_archives_dir, out_file_path):
        self._flibusta_archives_dir = flibusta_archives_dir
        self._out_file_path = out_file_path

    def _iterate_on_archive_paths(self):
        for path in Path(self._flibusta_archives_dir).iterdir():
            if self._ARCHIVE_PATTERN.match(path.name):
                yield path

    def _iterate_on_raw_fb2_texts(self):
        archive_paths = tqdm(self._iterate_on_archive_paths(), desc='Archives')

        for archive_path in archive_paths:
            try:
                with ZipFile(archive_path, 'r') as zip_file:
                    for file_name in zip_file.namelist():
                        data = zip_file.read(file_name)
                        yield data
            except BadZipFile:
                _logger.warning(f'Bad zip file: {archive_path}')

    def _iterate_on_book_texts(self):
        raw_fb2_texts = tqdm(self._iterate_on_raw_fb2_texts(), desc='Books')

        for raw_fb2_text in raw_fb2_texts:
            book_soup = bs4.BeautifulSoup(raw_fb2_text, features="html.parser")
            lang_tag = book_soup.find('lang')

            if lang_tag and lang_tag.text.lower().strip() == 'ru':
                book_text = book_soup.text
                yield book_text

    def _iterate_on_book_dialogs(self):
        book_texts = self._iterate_on_book_texts()
        dialog_separators_set = set(self._DIALOG_SEPARATORS)

        for book_text in book_texts:
            book_text_lines = re.split('\n+', book_text)
            dialog = []

            for line in book_text_lines:
                line = line.strip()

                if len(line) > 2 and line[0] in dialog_separators_set:
                    line = unicodedata.normalize("NFKC", line)
                    split_line = self._AUTHOR_WORDS_SEPARATOR_PATTERN.split(line)
                    utterance = ''.join(chain(*islice(chunked(split_line, 2), None, None, 2)))
                    utterance = re.sub(r'^\W+', '', utterance)
                    dialog.append(utterance)
                else:
                    if len(dialog) >= self._MIN_N_UTTERANCES:
                        yield dialog

                    dialog = []

            if len(dialog) >= self._MIN_N_UTTERANCES:
                yield dialog

    def run(self):
        dialogs = tqdm(self._iterate_on_book_dialogs(), desc='Dialogs')
        out_file = open(self._out_file_path, 'w')

        for dialog in dialogs:
            payload = json.dumps(dialog, ensure_ascii=False)
            out_file.write(payload)
            out_file.write('\n')
