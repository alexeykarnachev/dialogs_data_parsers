import argparse

from dialogs_data_parsers.flibusta.author_words_annotation_generator import FlibustaAuthorWordsAnnotationGenerator


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--raw_dialogs_file_path', type=str, required=True)
    parser.add_argument('--out_file_path', type=str, required=True)
    parser.add_argument('--n_samples', type=int, required=True)
    parser.add_argument('--augment_p', type=float, required=False, default=0.3)

    args = parser.parse_args()
    return args


def main():
    args = _parse_args()
    samples_generator = FlibustaAuthorWordsAnnotationGenerator(raw_dialogs_file_path=args.raw_dialogs_file_path,
                                                               out_file_path=args.out_file_path,
                                                               n_samples=args.n_samples,
                                                               augment_p=args.augment_p)

    samples_generator.run()


if __name__ == '__main__':
    main()
