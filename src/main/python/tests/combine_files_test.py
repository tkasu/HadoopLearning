import gzip
import os
import sys

# TODO, How can I move this to src/test/python instead?
cur_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(cur_path)

import pytest
from ncdc_analysis.preprocessing.combine_files_to_yearly import get_path_last_item
from ncdc_analysis.core.combine_files import combine_files


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


def test_file_combine_functionality(tmpdir):
    """Functionality test of combine_files_to_yearly.
    Simulates input data using pytest's tmpdir fixture, compressing data and writing it to temporary folders,
    running the functionality and checking the output by reading the files again."""
    test_lines_1990_file1 = "1990, foo\n1990, bar"
    test_lines_1990_file2 = "1990, biz\n1990, bizzier"
    test_lines_1991 = "1991, biz"

    test_lines_1990_file1_gz = gzip.compress(bytes(test_lines_1990_file1, "utf-8"))
    test_lines_1990_file2_gz = gzip.compress(bytes(test_lines_1990_file2, "utf-8"))
    test_lines_1991_gz = gzip.compress(bytes(test_lines_1991, "utf-8"))

    root_folder = tmpdir.mkdir("nooa")
    f1_folder = root_folder.mkdir("1990")
    f2_folder = root_folder.mkdir("1991")

    f1_1990 = f1_folder.join("file1-1990.gz")
    f2_1990 = f1_folder.join("file2-1990.gz")
    f1_1991 = f2_folder.join("file1-1991.gz")

    with open(f1_1990, "wb+") as f:
        f.write(test_lines_1990_file1_gz)
    with open(f2_1990, "wb+") as f:
        f.write(test_lines_1990_file2_gz)
    with open(f1_1991, "wb+") as f:
        f.write(test_lines_1991_gz)

    out_folder = tmpdir.mkdir("out")
    combine_files(root_folder, out_folder)

    with gzip.open(out_folder.join("1990.gz")) as combined_f_1990:
        content = combined_f_1990.read().decode("utf-8")
        try:
            assert content == (test_lines_1990_file1 + test_lines_1990_file2)
        except AssertionError:  # combine_files does not guarantee order
            assert content == (test_lines_1990_file2 + test_lines_1990_file1)

    with gzip.open(out_folder.join("1991.gz")) as combined_f_1991:
        content = combined_f_1991.read().decode("utf-8")
        assert content == test_lines_1991
