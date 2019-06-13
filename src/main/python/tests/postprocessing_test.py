import pandas as pd
import pytest
from ncdc_analysis.postprocessing.map_reduce_utils import clean_mapr_results


def test_mapr_clean_str():
    """Tests input_type == str"""
    test_mapr_data = "key1\tval_a, val_b, val_c\nkey2\tval_d, val_e, val_f"
    expected_result = pd.DataFrame(index=["key1", "key2"], data={"col_1": ["val_a", "val_d"],
                                                                 "col_2": ["val_b", "val_e"],
                                                                 "col_3": ["val_c", "val_f"]})
    test_result = clean_mapr_results(test_mapr_data, col_names=["col_1", "col_2", "col_3"])
    assert test_result.equals(expected_result)


def test_mapr_clean_list_of_str():
    """Tests input type == List[str]"""
    test_mapr_data = ["key1\tval_a, val_b, val_c\nkey2\tval_d, val_e, val_f",
                      "key3\tval_g, val_h, val_i\nkey4\tval_j, val_k, val_l"]
    expected_result = pd.DataFrame(index=["key1", "key2", "key3", "key4"],
                                   data={"col_1": ["val_a", "val_d", "val_g", "val_j"],
                                         "col_2": ["val_b", "val_e", "val_h", "val_k"],
                                         "col_3": ["val_c", "val_f", "val_i", "val_l"]})
    test_result = clean_mapr_results(test_mapr_data, col_names=["col_1", "col_2", "col_3"])
    assert test_result.equals(expected_result)


def test_mapr_clean_no_col_names():
    """Tests without provided col names"""
    test_mapr_data = "key1\tval_a, val_b, val_c\nkey2\tval_d, val_e, val_f"
    expected_result = pd.DataFrame(index=["key1", "key2"], data={0: ["val_a", "val_d"],
                                                                 1: ["val_b", "val_e"],
                                                                 2: ["val_c", "val_f"]})
    test_result = clean_mapr_results(test_mapr_data)
    assert test_result.equals(expected_result)
