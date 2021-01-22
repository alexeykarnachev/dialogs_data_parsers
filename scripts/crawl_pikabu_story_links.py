import argparse
import asyncio
import datetime
import os

from dialogs_data_parsers.common.log_config import prepare_logging
from dialogs_data_parsers.pikabu.story_links_crawler import PikabuStoryLinksCrawler


def _parse_args():
    parser = argparse.ArgumentParser(description='Crawls pikabu story links and stores them in day-separated files.')
    parser.add_argument(
        '--root_dir',
        type=str,
        required=True,
        help='Path to the root pikabu results directory. Sub-directory with links will be created there.')
    parser.add_argument(
        '--start_day', type=str, required=False, default='01-09-2010', help='Stories to crawl start day (%d-%m-%Y).')
    parser.add_argument(
        '--end_day',
        type=str,
        required=False,
        default=_get_default_end_day(),
        help='Stories to crawl end day (%d-%m-%Y).')
    parser.add_argument('--concurrency', type=int, required=False, default=12, help='Number of concurrent requests.')
    parser.add_argument('--timeout', type=int, required=False, default=10, help='Timeout in seconds.')
    parser.add_argument('--retries', type=int, required=False, default=5, help='Number of request retries.')
    parser.add_argument('--pikabu_section', type=str, required=False, default='best', help='Pikabu section to crawl.')

    args = parser.parse_args()
    return args


def _get_default_end_day():
    date = datetime.datetime.now().date() - datetime.timedelta(days=1)
    return date.strftime("%d-%m-%Y")


def main():
    args = _parse_args()

    out_dir = os.path.join(args.root_dir, 'story_links')
    logs_dir = os.path.join(args.root_dir, 'logs')
    prepare_logging(logs_dir, log_files_prefix='story_links_')
    crawler = PikabuStoryLinksCrawler(
        concurrency=args.concurrency,
        timeout=args.timeout,
        retries=args.retries,
        out_dir=out_dir,
        start_day=args.start_day,
        end_day=args.end_day,
        pikabu_section=args.pikabu_section)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(crawler.run())


if __name__ == '__main__':
    main()
