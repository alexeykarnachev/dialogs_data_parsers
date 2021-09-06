import json
import logging
import re
from typing import Optional

from treelib import Tree

from dialogs_data_parsers.utils import iterate_on_parts_by_condition

_logger = logging.getLogger(__name__)


class PikabuDialogsWithMetaIterator:
    def __init__(self, file_path, max_n_words_per_utterance, logging_period=10000):
        self._file_path = file_path
        self._logging_period = logging_period
        self._max_n_words_per_utterance = max_n_words_per_utterance

    def __iter__(self):
        with open(self._file_path) as file:
            n_samples_done = 0
            for n_lines_done, raw_line in enumerate(file, start=1):
                if self._logging_period and n_lines_done % self._logging_period == 0:
                    _logger.info(f'Pikabu lines: {n_lines_done}, samples: {n_samples_done}')

                line_data = json.loads(raw_line)
                dialog_tree = self._get_dialog_tree(line_data)
                dialogs = _iterate_on_dialogs_from_tree(dialog_tree)
                dialogs = set(dialogs)

                subdialogs = set()

                for dialog in dialogs:
                    dialog = tuple(dialog)
                    for n_utterances in range(2, len(dialog) + 1):
                        subdialog = tuple(dialog[:n_utterances])
                        subdialogs.add(_Dialog(subdialog))

                n_samples_done += len(subdialogs)
                for subdialog in subdialogs:
                    yield tuple(subdialog)

    def _get_dialog_tree(self, line_data):
        tree = Tree()
        tree.create_node(identifier=0)
        comments = line_data['comments']
        if comments:
            ids_and_comments = ((int(id_), comment) for id_, comment in comments.items())
            ids_and_comments = sorted(ids_and_comments, key=lambda x: x[0])

            for id_, comment_json in ids_and_comments:
                parent_id = int(comment_json['parent_id'])
                comment = comment_json['text']
                comment = comment.replace('\n', ' ')
                comment_text = self._process_comment(comment)
                if comment_text:
                    meta = comment_json.get('meta')
                    data = {'text': comment_text, 'meta': meta}
                else:
                    data = None

                tree.create_node(identifier=id_, parent=parent_id, data=data)

        return tree

    def _process_comment(self, text) -> Optional[str]:
        if not text:
            return None

        if '@' in text or 'http' in text:
            return None

        n_words = len(re.findall(r'\w+', text))
        if n_words > self._max_n_words_per_utterance:
            return None

        text = text.strip()
        if text.startswith('Комментарий удален.'):
            return None

        return text


class PikabuDialogsIterator(PikabuDialogsWithMetaIterator):
    def __init__(self, file_path, max_n_words_per_utterance, logging_period=10000):
        super().__init__(file_path, max_n_words_per_utterance=max_n_words_per_utterance, logging_period=logging_period)

    def __iter__(self):
        for subdialog in super().__iter__():
            utterances = [utterance['text'] for utterance in subdialog]
            yield utterances


_UPVOTE_DOWNVOTE_REGEX = re.compile(r'av=(\d+):(\d+)')
_MIN_N_VOTES = 30
_LABEL_THRESHOLD = 0.8
UNK_RATING_LABEL = 0
BALANCED_RATING_LABEL = 1
HIGH_RATING_LABEL = 2
LOW_RATING_LABEL = 3


class PikabuDialogsWithResponseRatingIterator(PikabuDialogsWithMetaIterator):
    def __init__(self, file_path, max_n_words_per_utterance, logging_period=10000):
        super().__init__(file_path, max_n_words_per_utterance=max_n_words_per_utterance, logging_period=logging_period)

    def __iter__(self):
        for subdialog in super().__iter__():
            utterances = [utterance['text'] for utterance in subdialog]
            response_meta = subdialog[-1]['meta']
            response_rating_label = _get_rating_from_meta(response_meta)
            if response_rating_label is not None:
                yield {'dialog': utterances, 'label': response_rating_label}


def _get_rating_from_meta(response_meta):
    upvote_downvote = _UPVOTE_DOWNVOTE_REGEX.findall(response_meta)
    label = UNK_RATING_LABEL
    if len(upvote_downvote) == 1:
        upvote_downvote = upvote_downvote.pop()
        upvote, downvote = map(int, upvote_downvote)
        n_votes = upvote + downvote
        if n_votes >= _MIN_N_VOTES:
            upvote_ratio = upvote / n_votes
            if upvote_ratio > _LABEL_THRESHOLD:
                label = HIGH_RATING_LABEL
            elif (1 - upvote_ratio) > _LABEL_THRESHOLD:
                label = LOW_RATING_LABEL
            else:
                label = BALANCED_RATING_LABEL

    return label


class _Dialog:
    def __init__(self, utterance_dicts):
        self._utterance_dicts = utterance_dicts

    def __iter__(self):
        yield from self._utterance_dicts

    def __hash__(self):
        return hash(tuple(d['text'] for d in self))

    def __eq__(self, other):
        return hash(self) == hash(other)


def _iterate_on_dialogs_from_tree(dialog_tree: Tree):
    for path in dialog_tree.paths_to_leaves():
        path = path[1:]  # Skip dummy root node
        dialog = [dialog_tree[p].data for p in path]

        # Split dialog on parts by empty utterance:
        dialogs = iterate_on_parts_by_condition(dialog, lambda utterance: not utterance)

        for dialog in dialogs:
            yield _Dialog(dialog)
