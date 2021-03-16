import json
import logging
from typing import Optional

from treelib import Tree

from dialogs_data_parsers.utils import iterate_on_parts_by_condition

_logger = logging.getLogger(__name__)


class _Dialog:
    def __init__(self, utterance_dicts):
        self._utterance_dicts = utterance_dicts

    def __iter__(self):
        yield from self._utterance_dicts

    def __hash__(self):
        return hash(tuple(d['text'] for d in self))


class PikabuDialogsWithMetaIterator:
    def __init__(self, file_path, logging_period=10000):
        self._file_path = file_path
        self._logging_period = logging_period

    def __iter__(self):
        with open(self._file_path) as file:
            n_samples_done = 0
            for n_lines_done, raw_line in enumerate(file, start=1):

                if self._logging_period and n_lines_done % self._logging_period == 0:
                    _logger.info(f'Pikabu lines: {n_lines_done}, samples: {n_samples_done}')

                line_data = json.loads(raw_line)
                dialog_tree = self._get_dialog_tree(line_data)
                dialogs = self._iterate_on_dialogs_from_tree(dialog_tree)
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
                    data = {'text': comment_text, 'meta': comment_json['meta']}
                else:
                    data = None

                tree.create_node(identifier=id_, parent=parent_id, data=data)

        return tree

    @staticmethod
    def _iterate_on_dialogs_from_tree(dialog_tree: Tree):
        for path in dialog_tree.paths_to_leaves():
            path = path[1:]  # Skip dummy root node
            dialog = [dialog_tree[p].data for p in path]

            # Split dialog on parts by empty utterance:
            dialogs = iterate_on_parts_by_condition(dialog, lambda utterance: not utterance)

            for dialog in dialogs:
                yield _Dialog(dialog)

    @staticmethod
    def _process_comment(text) -> Optional[str]:
        if not text:
            return None

        text = text.strip()
        if text.startswith('Комментарий удален.'):
            return None

        return text


class PikabuDialogsIterator(PikabuDialogsWithMetaIterator):
    def __init__(selt, file_path, logging_period=10000):
        super().__init__(file_path, logging_period)

    def __iter__(self):
        for subdialog in super().__iter__():
            yield from (utterance['text'] for utterance in subdialog)

