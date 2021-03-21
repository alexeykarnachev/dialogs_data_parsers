from collections import namedtuple
from itertools import cycle
from multiprocessing import Manager, Process
from os import cpu_count
from pathlib import Path
import struct

from more_itertools import chunked

from dialogs_data_parsers.dialogs_tokenizer import DialogsTokenizer

_WRITE_CHUNK_SIZE = 10000
_DATA_FILE_NAME = 'data.bin'
_OFFSETS_FILE_NAME = 'offsets.bin'
_RESPONSE_LENGTHS_FILE_NAME = 'response_lengths.bin'
_LABELS_FILE_NAME = 'labels.bin'
_META_FILE_NAME = 'meta.json'


def get_data_file_path(out_dir_path):
    return out_dir_path / _DATA_FILE_NAME


def get_offsets_file_path(out_dir_path):
    return out_dir_path / _OFFSETS_FILE_NAME


def get_response_lengths_file_path(out_dir_path):
    return out_dir_path / _RESPONSE_LENGTHS_FILE_NAME


def get_labels_file_path(out_dir_path):
    return out_dir_path / _LABELS_FILE_NAME


def get_meta_file_path(out_dir_path):
    return out_dir_path / _META_FILE_NAME


class DialogsDatasetSerializer:
    def __init__(self, dialogs, out_dir_path, tokenizer_name_or_path, max_n_tokens, max_n_utterances, n_workers=None):
        self._out_dir_path = Path(out_dir_path)
        self._dialogs = dialogs
        self._tokenizer_name_or_path = tokenizer_name_or_path
        self._max_n_tokens = max_n_tokens
        self._max_n_utterances = max_n_utterances
        self._n_workers = n_workers or cpu_count()

        self._data_file_path = get_data_file_path(self._out_dir_path)
        self._offsets_file_path = get_offsets_file_path(self._out_dir_path)
        self._response_lengths_file_path = get_response_lengths_file_path(self._out_dir_path)
        self._labels_file_path = get_labels_file_path(self._out_dir_path)
        self._meta_file_path = get_meta_file_path(self._out_dir_path)

        manager = Manager()
        self._lock = manager.Lock()
        self._offset = manager.Value('i', 0)
        self._n_samples = manager.Value('i', 0)
        self._pos_and_label_to_id = manager.dict()

        self._data_file = None
        self._response_lengths_file = None
        self._offsets_file = None

    def start(self):
        worker_processes = []
        for worker_id in range(self._n_workers):
            worker_process = Process(target=self._run, args=(worker_id, ))
            worker_process.start()
            worker_processes.append(worker_process)

        for worker_process in worker_processes:
            worker_process.join()

        self._write_meta()

    def _run(self, worker_id):
        self._data_file = open(self._data_file_path, 'wb')
        self._response_lengths_file = open(self._response_lengths_file_path, 'wb')
        self._offsets_file = open(self._offsets_file_path, 'wb')

        worker_data_sample_chunks = chunked(self._iterate_on_worker_data_samples(worker_id), _WRITE_CHUNK_SIZE)
        for data_samples in worker_data_sample_chunks:
            with self._lock:
                offset = self._offset.value
                n_samples = self._n_samples.value
                pos_and_label_to_id = dict(self._pos_and_label_to_id)
                for data_sample in data_samples:
                    offset, pos_and_label_to_id = self._write_data(data_sample=data_sample,
                                                                   offset=offset,
                                                                   pos_and_label_to_id=pos_and_label_to_id)
                    n_samples += 1

                self._offset.value = offset
                self._n_samples = n_samples
                self._pos_and_label_to_id.update(pos_and_label_to_id)

    def _iterate_on_worker_data_samples(self, worker_id):
        tokenizer = DialogsTokenizer(self._tokenizer_name_or_path,
                                     max_n_tokens=self._max_n_tokens,
                                     max_n_utterances=self._max_n_utterance)
        worker_ids_cycle = cycle(range(self._n_workers))
        for worker_id_, dialog in zip(worker_ids_cycle, self._dialogs):
            if worker_id_ == worker_id:
                encoded_dialog, n_utterances, is_incomplete = tokenizer.encode_dialog(dialog.utterances)
                if not is_incomplete and n_utterances >= 2:
                    response_length = get_response_length(encoded_dialog, tokenizer.end_of_speaker_1_token_id)
                    data_sample = _DataSample(encoded_dialog=encoded_dialog,
                                              response_length=response_length,
                                              labels=dialog.labels)
                    yield data_sample

    def _write_data(self, data_sample, offset, pos_and_label_to_id):
        n_bytes = self._data_file.write(data_sample.encoded_dialog.tobytes())
        new_offset = offset + n_bytes
        self._offsets_file.write(struct.pack('>Q', new_offset))
        self._response_lengths_file.write(struct.pack('>H', data_sample.response_length))
        pos_and_label_to_id = self._write_labels(data_sample.labels, pos_and_label_to_id)

        return new_offset, pos_and_label_to_id

    def _write_labels(self, labels, pos_and_label_to_id):
        pos_and_label_to_id = pos_and_label_to_id.copy()
        n_labels = len(labels)
        if self._n_labels is not None and self._n_labels != n_labels:
            raise ValueError(
                f'Inconsistent number of labels. Earlier we saw {self._n_labels} labels, but now {n_labels}')

        if self._n_labels:
            label_ids = []
            for pos_and_label in enumerate(labels):
                if pos_and_label not in pos_and_label_to_id:
                    new_id = sum(pos_and_label_[0] == pos_and_label[0] for pos_and_label_ in pos_and_label_to_id.keys())
                    pos_and_label_to_id[pos_and_label] = new_id

                label_ids.append(pos_and_label_to_id[pos_and_label])

            self._labels_file.write(struct.pack('>h', *label_ids))

        return pos_and_label_to_id


_DataSample = namedtuple('_DataSample', ('encoded_dialog', 'response_length', 'labels'))


def get_response_length(encoded_dialog, end_of_speaker_1_token_id):
    response_length = (encoded_dialog == end_of_speaker_1_token_id)[::-1].argmax()
    assert response_length
    return response_length
