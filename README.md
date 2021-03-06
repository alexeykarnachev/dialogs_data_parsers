# Dialogs Data Parsers
Репозиторий для парсинга диалоговых данных.
**Только для исследовательских целей!**

```shell script
pip install -U -e .
```

## Pikabu
#### Parsing
Для сбора и парсинга данных с [pikabu](https://pikabu.ru) нужно сперва собрать ссылки на истории:
```shell script
python scripts/crawl_pikabu_story_links.py --root_dir path/to/output/dir 
```
- *--root_dir* - Путь к root директории, в которой будут лежать результаты парсинга.

Остальные аргументы можно посмотреть в скрипте (для них есть дефолтные значения).

После того, как ссылки на истории собраны, можно запускать парсер:
```shell script
python scripts/crawl_pikabu_stories.py --root_dir path/to/output/dir 
```
- *--root_dir* - Путь к root директории, в которой лежат ссылки на истории (тот же самый путь, что указывали в 
предыдущем скрипте).

Парсинг всего pikabu длится примерно неделю. Скрипт устойчив к прерываниям. Если по какой-то причине
парсинг прервался, то его можно перезапустить, указав тот же самый *--root_dir*. Парсинг продолжится с
того же места, где был прерван.

#### Data format
Результатом парсинга pikabu является jsonl файл. Каждая строчка - отдельный json со структурой 
(пример изображён с индентацией, но в настоящем файле этот Json будет записан в одну строку):
```json
{
 "url": "story url",
 "story": {
  "title": "story title",
  "text": "story text",
  "user_nick": "story author",
  "tags": ["tag1", "tag2", "tag3"],
  "comments_count": 42,
  "shares": 10,
  "saves": 228,
  "timestamp": "2017-04-21T11:38:56+03:00",
  "rating": 5
 },
 "comments": {
  "8811": {
   "text": "comment text",
   "parent_id": 0,
   "children": [8812, 8813]
  },
  "8812": {
   "text": "comment text",
   "parent_id": 8811,
   "children": [8814, 8815, 8816] 
  }
 }
}
```
Несколько важных моментов:
- Ключи в словаре `comments` - это id комментариев. Как ключи, они имеют текстовый формат, однако, id, которые
записаны в поля `parent_id` и `children` - имеют integer тип. Учитывайте это во время парсинга файла.
- Если у комментария `parent_id` равен 0, то это значит, что у комментария нет родителя (в данном случае саму историю
можно воспринимать как родителя).
- Комментарии хранятся в формате дерева. Такой формат можно распарсить в виде диалогов. Пример парсера в
`examples/pikabu_dialogs_iterator`

## Flibusta
#### Parsing
Для того, чтобы распарсить диалоги из книг с флибусты нужен дамп книг. Ссылку на дамп я здесь, разумеется,
прикладывать не буду :)

Дапм представляет из себя директорию с множеством .zip архивов примерно с такими названиями: `f.fb2-188228-190111.zip`.
Каждый архив в свою очередь содержит множество файлов формата `fb2`.

Для парсинга диалогов используется скрипт:
```shell script
python parse_flibusta_dialogs.py --flibusta_archives_dir path/to/flibusta/dir/with/archives --out_file_path path/to/dialogs/results/file --logs_dir path/to/logs/dir
```
- *--flibusta_archives_dir* - Путь к root директории со всеми архивами;
- *--out_file_path* - Путь к выходному jsonl файлу с диалогами;
- *--logs_dir* - Путь к директории, куда будут писаться логи.

Парсинг 130 архивов длится примерно 13 часов и это примерно 40-50 миллионов диалогов. Можно переписать на мультипроцессинге
и парсинг будет за 2 часа. Но мне лень.

#### Data format
Результатом парсинга flibusta является jsonl файл. Каждая строчка этого файла - просто список сообщений:
```json
["Привет, как дела?", "Нормально", "Ясно, понятно"]
```

По идее, в этих данных должны быть отфильтрованы слова автора, но возможно иногда они будут попадаться.
Плюс, возможны другие аномалии. Но беглый ручной осмотр пары сотен диалогов ничего странного не выявил.