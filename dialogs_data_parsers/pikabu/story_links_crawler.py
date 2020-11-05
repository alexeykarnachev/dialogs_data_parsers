import asyncio
import datetime
import json
import logging
import os
from pathlib import Path

import aiofiles
import bs4
from more_itertools import chunked

from dialogs_data_parsers.common.crawler import Crawler

_logger = logging.getLogger(__name__)
_DAYS_CHUNK_SIZE = 30


def _get_days_range(start_day, end_day):
    start = datetime.datetime.strptime(start_day, "%d-%m-%Y")
    end = datetime.datetime.strptime(end_day, "%d-%m-%Y")
    dates_generated = [start + datetime.timedelta(days=x) for x in range(0, (end - start).days + 1)]
    days = [date.strftime("%d-%m-%Y") for date in dates_generated]
    return days


class PikabuStoryLinksCrawler(Crawler):
    def __init__(self, concurrency, timeout, retries, out_dir, start_day, end_day, pikabu_section):
        super().__init__(concurrency=concurrency, timeout=timeout, retries=retries)

        self._out_dir = out_dir
        self._start_day = start_day
        self._end_day = end_day
        self._pikabu_section = pikabu_section
        self._n_total_links = 0

    async def run(self):
        Path(self._out_dir).mkdir(exist_ok=True, parents=True)
        days_range = _get_days_range(self._start_day, self._end_day)
        parsed_days = [path.name for path in Path(self._out_dir).iterdir()]
        days_range = set(days_range) - set(parsed_days)

        for days_chunk in chunked(days_range, n=_DAYS_CHUNK_SIZE):
            coroutines = [self._crawl(day) for day in days_chunk]
            await asyncio.gather(*coroutines)

    async def _crawl(self, day):
        links = await self._get_story_links(day=day)
        out_file_path = os.path.join(self._out_dir, day)
        async with aiofiles.open(out_file_path, "w") as f:
            await f.write('\n'.join(links))
            await f.flush()

    async def _get_story_links(self, day):
        current_page_id = 1
        links = set()
        headers = _get_headers(day=day, pikabu_section=self._pikabu_section)
        url = _get_url(day=day, pikabu_section=self._pikabu_section)

        while True:
            old_number_of_links = len(links)
            params = _get_params(page_number=current_page_id)
            response_text = await self.perform_request(url=url, headers=headers, params=params, method='get')
            stories = json.loads(response_text)['data']['stories']
            story_soups = [bs4.BeautifulSoup(s['html'], features="html.parser") for s in stories]

            for story_soup in story_soups:
                link_element = story_soup.find('a', {'class': 'story__title-link'})
                if link_element is not None:
                    href = link_element.get('href')
                    if href:
                        links.add(href)

            new_number_of_links = len(links)

            if old_number_of_links < new_number_of_links:
                _logger.debug(f'Day: {day}, links obtained: {new_number_of_links}, pages scrolled: {current_page_id}')
                current_page_id += 1
            else:
                break

        self._n_total_links += len(links)
        _logger.info(f'Day: {day} done, total number of links: {self._n_total_links}')

        return links


def _get_headers(day, pikabu_section):
    headers = {'referer': f'https://pikabu.ru/{pikabu_section}/{day}'}
    return headers


def _get_params(page_number):
    params = (('twitmode', '1'), ('of', 'v2'), ('page', f'{page_number}'), ('_', '1574097199724'))
    return params


def _get_url(day, pikabu_section):
    url = f'https://pikabu.ru/{pikabu_section}/{day}'
    return url
