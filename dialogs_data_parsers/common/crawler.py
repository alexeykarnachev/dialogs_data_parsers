import asyncio
import logging
from functools import partial
from typing import Optional

import aiohttp

_logger = logging.getLogger(__name__)


class Crawler:
    def __init__(self, concurrency, timeout, retries):
        self._timeout = timeout
        self._retries = retries

        self._semaphore = asyncio.BoundedSemaphore(concurrency)

    async def perform_request(self, url, headers=None, data=None, params=None, method='get') -> Optional[str]:
        """Requests a page and returns content."""
        _logger.debug(f'Requesting page: {url}')
        i_retry = 0
        async with self._get_session(headers=headers) as session:
            while i_retry < self._retries:
                try:
                    method = session.get if method == 'get' else partial(session.post, data=data)
                    async with self._semaphore, method(url, allow_redirects=False, params=params) as response:
                        text = await response.text()
                        _logger.debug(f'Page source obtained: {url}')
                        return text
                except asyncio.TimeoutError:
                    i_retry += 1
                    _logger.warning(f'Timeout for page [{i_retry}/{self._retries}]: {url}')
            else:
                _logger.warning(f'Max number of retries exceeded for page: {url}')
                return None

    def _get_session(self, headers):
        connector = aiohttp.TCPConnector()
        timeout = aiohttp.ClientTimeout(total=self._timeout)
        session = aiohttp.ClientSession(connector=connector, timeout=timeout, headers=headers)

        return session
