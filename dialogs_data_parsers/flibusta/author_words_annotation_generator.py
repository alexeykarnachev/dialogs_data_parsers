from itertools import chain, cycle
import json
from pathlib import Path
import random
import re

from dialogs_data_parsers.flibusta.dialogs_parser import DIALOG_SEPARATORS

_PERSON_FLAG = 0
_AUTHOR_FLAG = 1
_PUNCT_FLAG = 2
_DASH_FLAG = 3
_SPLIT_FLAGS = (_PERSON_FLAG, _PUNCT_FLAG, _DASH_FLAG, _AUTHOR_FLAG, _PUNCT_FLAG, _DASH_FLAG)

_DIALOGS_SEPARATORS_SET = set(DIALOG_SEPARATORS)
_AUTHOR_WORDS_SEPARATOR_PATTERN = re.compile(f'([.,!?:;]+)(\s?[{DIALOG_SEPARATORS}])')
_AUGMENT_PUNCT_CHOICES = list(set(chain(*[[symbol * i for i in range(0, 4)] for symbol in '.,!?:; '])))
_AUGMENT_DASH_CHOICES = list(
    set(chain(*[[' ' * i + symbol for i in range(0, 4)] for symbol in list(_DIALOGS_SEPARATORS_SET) + [' ']])))


class FlibustaAuthorWordsAnnotationGenerator:
    def __init__(self, raw_dialogs_file_path, out_file_path, n_samples, augment_p):
        self._raw_dialogs_file_path = raw_dialogs_file_path
        self._augment_p = augment_p
        self._out_file_path = Path(out_file_path)
        self._n_samples = int(n_samples)

    def run(self):
        utterances = self._iterate_on_utterances()
        n_samples_done = 0

        self._out_file_path.parent.mkdir(exist_ok=True, parents=True)

        with open(self._out_file_path, 'w') as out_file:
            for utterance in utterances:
                augmented_split_utterance_and_flags = self._generate_augmented_split_utterance_and_flags(utterance)
                if len(augmented_split_utterance_and_flags) == 0:
                    continue
                payload = json.dumps(augmented_split_utterance_and_flags, ensure_ascii=False)
                out_file.write(payload)
                out_file.write('\n')
                n_samples_done += 1

                if n_samples_done == self._n_samples:
                    break

                if n_samples_done % 10000 == 0:
                    print(f'Samples: {n_samples_done}/{self._n_samples}')

    def _iterate_on_utterances(self):
        with open(self._raw_dialogs_file_path) as file:
            for line in file:
                dialog = json.loads(line)
                for utterance in dialog:
                    yield utterance

    def _generate_augmented_split_utterance_and_flags(self, utterance):
        split_utterance = _AUTHOR_WORDS_SEPARATOR_PATTERN.split(utterance)
        split_utterance = [utterance for utterance in split_utterance if len(utterance)]

        augmented_split_utterance = []
        augmented_split_utterance_flags = []

        for sub_utterance, flag in zip(split_utterance, cycle(_SPLIT_FLAGS)):
            if flag != _PUNCT_FLAG and flag != _DASH_FLAG:
                augmented_split_utterance.append(sub_utterance)
                augmented_split_utterance_flags.append(flag)
            else:
                if random.random() <= self._augment_p:
                    choices = _AUGMENT_PUNCT_CHOICES if flag == _PUNCT_FLAG else _AUGMENT_DASH_CHOICES
                    sub_utterance = random.choice(choices)

                augmented_split_utterance[-1] += sub_utterance

        augmented_split_utterance_and_flags = list(zip(augmented_split_utterance, augmented_split_utterance_flags))

        return augmented_split_utterance_and_flags
