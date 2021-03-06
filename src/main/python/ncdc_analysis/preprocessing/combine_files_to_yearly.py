from dataclasses import dataclass
from glob import glob
import gzip
import os
from typing import List
import shutil
import sys


@dataclass
class NcdcFolder:
    year: str
    path: str


def get_path_last_item(path: str) -> str:
    """Get item (file or folder) from a path."""
    return os.path.basename(os.path.normpath(path))


def get_ncdc_folders(ncdc_path: str) -> List[NcdcFolder]:
    """NCDC data is formatted in folders name by year number.
    This functions returns the folder paths and the folder names."""
    folder_mask = "19*/"
    path_mask = os.path.join(ncdc_path, folder_mask)
    folders = glob(path_mask)
    folder_names = map(get_path_last_item, folders)
    folder_tuples = zip(folder_names, folders)
    ncdc_folders: List[NcdcFolder] = list(map(lambda t: NcdcFolder(*t), folder_tuples))
    return ncdc_folders


def get_gz_files(folder_path: str) -> List[str]:
    return glob(os.path.join(folder_path, "*-19*.gz"))


def compress_existing_file(file, delete_old_file=False, new_file_name=None):
    """Compresses existing file. If no new file name is given, just adds .gz to the end."""
    if not new_file_name:
        new_file_name = file + ".gz"
    with open(file, "rb") as f_in:
        with gzip.open(new_file_name, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    if delete_old_file:
        os.remove(file)


def combine_gz_files_to_one(folder, output_file):
    """Finds all .gz files in a given folder, combines the data of the files and compresses the data again."""
    gz_files = get_gz_files(folder.path)
    with open(output_file, "wb+") as outfile:
        for file in gz_files:
            with gzip.open(file, "rb") as infile:
                outfile.write(infile.read())
    compress_existing_file(output_file, delete_old_file=True)
