import os
import sys
from pathlib import Path


def create_directory_tree():
    docs_path = Path('data')
    if not os.path.isdir(docs_path):
        try:
            docs_path.mkdir(parents=True)
            docs_path.joinpath('external').mkdir()
            docs_path.joinpath('interim').mkdir()
            docs_path.joinpath('processed').mkdir()
        except Exception as e:
            print(f'Error in line {sys.exc_info()[-1].tb_lineno}. {e}.')
    else:
        print('Directory already exists.')
