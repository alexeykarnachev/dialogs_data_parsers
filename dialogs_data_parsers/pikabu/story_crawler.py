import asyncio
import copy
import json
import logging
import re
from pathlib import Path

import aiofiles
import bs4
from more_itertools import chunked

from dialogs_data_parsers.common.crawler import Crawler

_logger = logging.getLogger(__name__)
_GET_COMMENTS_URL = 'https://pikabu.ru/ajax/comments_actions.php'
_URLS_CHUNK_SIZE = 1000


def iterate_on_urls(story_links_dir):
    for story_links_file in Path(story_links_dir).iterdir():
        with open(story_links_file) as file:
            for url in file:
                yield url.strip()


class PikabuStoryCrawler(Crawler):
    def __init__(self, concurrency, timeout, retries, story_links, out_file_path):
        super().__init__(concurrency=concurrency, timeout=timeout, retries=retries)

        self._out_file_path = out_file_path
        self._parsed_urls = self._get_parsed_urls()
        self._all_urls = set(story_links)
        self._urls_to_parse = self._all_urls.difference(self._parsed_urls)
        self._n_urls_to_parse = len(self._urls_to_parse)

    @classmethod
    def from_story_links_dir(cls, concurrency, timeout, retries, story_links_dir, out_file_path):
        story_links = iterate_on_urls(story_links_dir)
        return cls(
            concurrency=concurrency,
            timeout=timeout,
            retries=retries,
            story_links=story_links,
            out_file_path=out_file_path
        )

    async def run(self):
        for urls_chunk in chunked(self._urls_to_parse, n=_URLS_CHUNK_SIZE):
            coroutines = [self._crawl(url) for url in urls_chunk]
            await asyncio.gather(*coroutines)

    def _get_parsed_urls(self):
        urls = set()
        if Path(self._out_file_path).is_file():
            with open(self._out_file_path) as file:
                for line in file:
                    url = line.split(',', maxsplit=1)[0].split('"')[-2]
                    assert url.startswith('https')
                    urls.add(url)

        return urls

    async def _crawl(self, url):
        _logger.debug(f'Crawling story: {url}')
        try:
            result = await self._get_story_and_comments(url=url)
        except Exception:
            _logger.exception(f'Failed to crawl story: {url}')
            result = None

        if result is None:
            _logger.debug(f'Result is None for story: {url}')
            return

        async with aiofiles.open(self._out_file_path, "a") as f:
            result_str = json.dumps(result, ensure_ascii=False)
            await f.write(result_str + '\n')
            await f.flush()
            _logger.debug(f'Story crawled and saved: {url}')

    async def _get_story_and_comments(self, url):
        story_id = url.split('_')[-1]
        story_html = await self.perform_request(url, headers=_get_headers(), method='get')

        if not story_html:
            return None

        story_soup = bs4.BeautifulSoup(story_html, features="html.parser")

        # Page not exists (deleted)
        if story_soup.find('div', {'class': 'app-404'}):
            _logger.debug(f'404 for story: {url}')
            return {'url': url, 'story': None, 'comments': []}

        story = _parse_story_soup(story_soup)

        parser = _CommentsParser()
        start_comment_id = 0
        prev_n_comments_parsed = None
        while prev_n_comments_parsed != parser.n_comments_parsed:
            data = _get_payload_data(story_id, start_comment_id)
            headers = _get_headers(url)
            result = await self.perform_request(_GET_COMMENTS_URL, headers=headers, data=data, method='post')

            _logger.debug(f'Parsing result for story: {url}')

            result_data = json.loads(result)['data']
            start_comment_id = result_data['last_id']
            prev_n_comments_parsed = parser.n_comments_parsed

            for comment_data in result_data['comments']:
                comment_soup = bs4.BeautifulSoup(comment_data['html'], features="html.parser")
                parser.parse_comment_and_children(comment_soup)

            _logger.debug(f'{parser.n_comments_parsed} comments parsed: {url}')

        self._n_urls_to_parse -= 1
        _logger.info(f'{url} Comments: {parser.n_comments_parsed}, Left: {self._n_urls_to_parse}')

        result = {'url': url, 'story': story, 'comments': parser.id_to_comment}
        return result


class _CommentsParser:
    def __init__(self):
        self._id_to_comment = {}

    @property
    def n_comments_parsed(self):
        return len(self._id_to_comment)

    @property
    def id_to_comment(self):
        id_to_comment = copy.deepcopy(self._id_to_comment)
        for comment in id_to_comment.values():
            comment['children'] = sorted(list(comment['children']))

        return id_to_comment

    def parse_comment_and_children(self, comment_soup):
        comment_soups = comment_soup.find_all('div', {'class': 'comment'})

        for comment_soup in comment_soups:
            comment = _parse_comment_soup(comment_soup)
            self._id_to_comment[comment['id']] = comment
            parent_id = comment['parent_id']
            if parent_id != 0:
                self._id_to_comment[parent_id]['children'].add(comment['id'])


def _parse_comment_soup(soup):
    body = soup.find('div', {'class': 'comment__body'})
    meta = soup['data-meta']

    id_ = int(_get_meta_tag(meta, r'^id=(\d+),', raise_if_not_found=True))
    pid = int(_get_meta_tag(meta, r',pid=(\d+),', raise_if_not_found=True))
    date = _get_meta_tag(meta, r',d=(.+),de=')
    rating = int(_get_meta_tag(meta, r'r=(\d+),', default=0))

    comment = {
        'user_nick': body.find('span', {'class': 'user__nick'}).get_text(' '),
        'text': body.find('div', {'class': 'comment__content'}).get_text('\n'),
        'id': id_,
        'parent_id': pid,
        'date': date,
        'rating': rating,
        'children': set()
    }
    return comment


def _get_meta_tag(meta, regex, default=None, raise_if_not_found=False):
    match = re.search(regex, meta)
    if not match and raise_if_not_found:
        raise ValueError(f"Can't find regex: {regex} in meta: {meta}")
    elif not match:
        return default
    else:
        return match.group(1)


def _get_headers(referer=None):
    headers = {
        'authority': 'pikabu.ru',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36',
        'origin': 'https://pikabu.ru'
    }
    if referer:
        headers['referer'] = referer
    return headers


def _get_payload_data(story_id, start_comment_id):
    return {'action': 'get_story_comments', 'story_id': story_id, 'start_comment_id': start_comment_id}


def _get_element_text(elem, separator='', default=None):
    return elem.get_text(separator) if elem else default


def _parse_story_soup(soup):
    story_main = soup.find('div', {'class': 'story__main'})

    title = _get_element_text(story_main.find('span', {'class': 'story__title-link'}), ' ')
    text = _get_element_text(story_main.find('div', {'class': 'story-block story-block_type_text'}), '\n')
    user_nick = _get_element_text(story_main.find('a', {'class': 'user__nick'}), ' ')
    tags = sorted(set(_get_element_text(tag, ' ') for tag in story_main.find_all('a', {'class': 'tags__tag'})))
    shares = int(_get_element_text(story_main.find('span', {'class': 'story__share-count'}), default=0))
    saves = int(_get_element_text(story_main.find('span', {'class': 'story__save-count'}), default=0))
    rating = int(_get_element_text(story_main.find('span', {'class': 'story__rating-count'}), default=0))

    comments_count = _get_element_text(story_main.find('span', {'class': 'story__comments-link-count'}), default='0')
    comments_count = int(re.findall(r'\d+', comments_count)[0])

    time_ = story_main.find('time') or dict()
    timestamp = time_.get('datetime')

    res = {
        "title": title,
        "text": text,
        "user_nick": user_nick,
        "tags": tags,
        "comments_count": comments_count,
        "shares": shares,
        "saves": saves,
        "timestamp": timestamp,
        "rating": rating
    }

    return res
