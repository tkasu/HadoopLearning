from io import StringIO
from typing import Union, List
import pandas as pd


def clean_spark_results(csv_data: Union[str, List[str]]) -> pd.DataFrame:
    """Reads spark result csv-data to pandas dataframe."""
    # Flatten List[str] -> str
    if not isinstance(csv_data, str):
        data: str = "\n".join(csv_data)
    else:
        data = csv_data

    csv_io = StringIO(data)
    df = pd.read_csv(csv_io)
    return df
