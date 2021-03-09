import pathlib

from setuptools import find_packages, setup

_THIS_DIR = pathlib.Path(__file__).parent


def _get_requirements():
    with (_THIS_DIR / 'requirements.txt').open() as fp:
        return fp.read()


setup(name='dialogs_data_parsers',
      version='0.0.1',
      install_requires=_get_requirements(),
      package_dir={'dialogs_data_parsers': 'dialogs_data_parsers'},
      packages=find_packages(exclude=['tests', 'tests.*']))
