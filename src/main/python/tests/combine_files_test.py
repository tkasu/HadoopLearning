import os
import sys

# TODO, How can I move this to src/test/python instead?
cur_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(cur_path)

import pytest
from ncdc_analysis.preprocessing.combine_files_to_yearly import get_path_last_item


def test_pytest():
    assert 1 == 1


def test_get_path_last_item_is_folder():
    path = "/this/is/a/path/folder/"
    last_item = get_path_last_item(path)
    assert last_item == "folder"


def test_get_path_last_item_is_file():
    path = "/this/is/a/path/folder/file.txt"
    last_item = get_path_last_item(path)
    assert last_item == "file.txt"
