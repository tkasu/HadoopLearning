import os
from typing import List
from ..preprocessing.combine_files_to_yearly import  get_ncdc_folders, combine_gz_files_to_one, NcdcFolder


def combine_files(input_folder, output_folder):
    """Combines NCDC Weather data from year-named folders including .gz files to one .gz file for each year."""
    folders: List[NcdcFolder] = get_ncdc_folders(input_folder)
    for folder in folders:
        output_file = os.path.join(output_folder, folder.year)
        combine_gz_files_to_one(folder, output_file)
