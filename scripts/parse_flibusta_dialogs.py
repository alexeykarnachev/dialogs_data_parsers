import argparse

from dialogs_data_parsers.common.log_config import prepare_logging
from dialogs_data_parsers.flibusta.dialogs_parser import FlibustaDialogsParser


def _parse_args():
    parser = argparse.ArgumentParser(description='Parses flibusta dialogs from flibusta fb2 archives.')
    parser.add_argument(
        '--flibusta_archives_dir', type=str, required=True,
        help='Path to the dir with flibusta zip archives. Each archive contains fb2 files.'
    )
    parser.add_argument(
        '--out_file_path', type=str, required=True,
        help='Path to the output dialogs file.'
    )
    parser.add_argument(
        '--logs_dir', type=str, required=True,
        help='Path to the logs directory.'
    )

    args = parser.parse_args()
    return args


def main():
    args = _parse_args()
    prepare_logging(args.logs_dir)
    parser = FlibustaDialogsParser(args.flibusta_archives_dir, args.out_file_path)
    parser.run()


if __name__ == '__main__':
    main()
