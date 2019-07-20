import click
from ..core.combine_files import combine_files


@click.command()
@click.option("--input", help="noaa folder which includes year-named folders")
@click.option("--output", help="the path where new year-named files will be created")
def combiner(input, output):
    """When fetching data with FTP from ftp://ftp.ncdc.noaa.gov/pub/data/noaa/ the data is splitted to small files.
    We reprocess the files for bigger chunks to increase the performance of our analysis-stack."""
    if input and output:
        combine_files(input, output)
    else:
        print(f"""Script to combine small .gz files fetched from ftp://ftp.ncdc.noaa.gov/pub/data/noaa/ 
        for large year-based files. 
        Usage: --input <input_path> --output <output_path> 
        See --help for parameter description.""")


if __name__ == "__main__":
    combiner()
