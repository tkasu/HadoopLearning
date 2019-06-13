import pandas as pd
from toolz.functoolz import pipe
from typing import List, Optional, Union


def clean_mapr_results(raw_data: Union[str, List[str]], col_names: Optional[List[str]] = None) -> pd.DataFrame:
    """Cleans mapreduce result strings. Also supports comma separated value array.
    Expects raw_data in following format which are by default outputted by MapReduce:
    'key1\tval_a, val_b, val_c\nkey2\tval_d, val_e, val_f'
    Input can be also List of such input strings"""
    def _split_lines(data):
        return data.split("\n")

    def _remove_empty(data):
        return filter(lambda row: row != "", data)

    def _split_keys_n_vals(data):
        return map(lambda line: line.split("\t"), data)

    def _split_val_csv(data):
        return map(lambda line: [line[0]] + line[1].split(","), data)

    def _strip_items(data):
        return map(lambda item: list(map(lambda x: x.strip(), item)), data)

    # Flatten List[str] -> str
    if not isinstance(raw_data, str):
        data: str = "\n".join(raw_data)
    else:
        data = raw_data
    clean_data = pipe(data, _split_lines, _remove_empty, _split_keys_n_vals, _split_val_csv, _strip_items, list)

    if not col_names:
        val_cnt = len(clean_data[0]) - 1  # Deduct 1 as the first value is the index
        col_names = list(range(val_cnt))
    df_col_names = ["index"] + col_names
    df = pd.DataFrame(data=clean_data, columns=df_col_names).set_index("index")
    return df
