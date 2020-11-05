import json

from treelib import Tree

with open('./tmp.jsonl') as file:
    for line in file:
        story = json.loads(line)

tree = Tree()
tree.create_node(identifier=0, data=story['story']['text'])
for comment in story['comments'].values():
    tree.create_node(identifier=int(comment['id']), parent=int(comment['parent_id']), data=comment['text'])

dialogs = []
for path in tree.paths_to_leaves():
    dialog = [tree[p].data for p in path]
    dialogs.append(dialog)
    print(dialog)
    print('\n\n')