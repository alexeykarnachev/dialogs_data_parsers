import json
from typing import Optional

from tqdm import tqdm
from treelib import Tree

from dialog_model.utils import iterate_on_parts_by_condition


class PikabuDialogsIterator:
    def __init__(self, file_path, min_n_messages_in_dialog=1, verbose=True):
        self._file_path = file_path
        self._min_n_messages_in_dialog = min_n_messages_in_dialog
        self._verbose = verbose

    def __iter__(self):
        with open(self._file_path) as file:
            file = tqdm(file, desc='Pikabu lines done') if self._verbose else file
            for raw_line in file:
                line_data = json.loads(raw_line)
                dialog_tree = self._get_dialog_tree(line_data)
                dialogs = self._iterate_on_dialogs_from_tree(dialog_tree)
                dialogs = set(tuple(dialog) for dialog in dialogs if len(dialog) >= self._min_n_messages_in_dialog)

                yield from dialogs

    def _get_dialog_tree(self, line_data):
        tree = Tree()
        tree.create_node(identifier=0)
        comments = line_data['comments']
        if comments:
            ids_and_comments = ((int(id_), comment) for id_, comment in comments.items())
            ids_and_comments = sorted(ids_and_comments, key=lambda x: x[0])

            for id_, comment in ids_and_comments:
                parent_id = int(comment['parent_id'])
                comment = comment['text']
                comment = comment.replace('\n', ' ')
                comment_text = self._process_comment(comment)
                tree.create_node(identifier=id_, parent=parent_id, data=comment_text)

        return tree

    @staticmethod
    def _iterate_on_dialogs_from_tree(dialog_tree: Tree):
        for path in dialog_tree.paths_to_leaves():
            path = path[1:]  # Skip dummy root node
            dialog = [dialog_tree[p].data for p in path]

            # Split dialog on parts by empty utterance:
            dialogs = iterate_on_parts_by_condition(dialog, lambda utterance: not utterance)

            yield from dialogs

    @staticmethod
    def _process_comment(text) -> Optional[str]:
        if not text:
            return None

        text = text.strip()
        if text.startswith('Комментарий удален.'):
            return None

        return text
