import argparse
import asyncio
import os

from dialogs_data_parsers.common.log_config import prepare_logging
from dialogs_data_parsers.pikabu.story_crawler import PikabuStoryCrawler


def _parse_args():
    parser = argparse.ArgumentParser(description='Crawls pikabu stories and stores them in one jsonl file.')
    parser.add_argument(
        '--root_dir',
        type=str,
        required=True,
        help='Path to the root pikabu results directory. Sub-directory with links will be created there.')
    parser.add_argument('--concurrency', type=int, required=False, default=12, help='Number of concurrent requests.')
    parser.add_argument('--timeout', type=int, required=False, default=10, help='Timeout in seconds.')
    parser.add_argument('--retries', type=int, required=False, default=5, help='Number of request retries.')

    args = parser.parse_args()
    return args


def main():
    args = _parse_args()

    out_file_path = os.path.join(args.root_dir, 'stories.jsonl')
    story_links_dir = os.path.join(args.root_dir, 'story_links')
    logs_dir = os.path.join(args.root_dir, 'logs')
    prepare_logging(logs_dir, log_files_prefix='stories_')

    crawler = PikabuStoryCrawler.from_story_links_dir(
        concurrency=args.concurrency,
        timeout=args.timeout,
        retries=args.retries,
        story_links_dir=story_links_dir,
        out_file_path=out_file_path)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(crawler.run())


if __name__ == '__main__':
    main()
