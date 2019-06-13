import boto3
import os
from urllib.parse import urlparse
import pandas as pd
from typing import Tuple, List, Dict, Optional
from ncdc_analysis.postprocessing.map_reduce_utils import clean_mapr_results


def s3_path_to_folder_and_key(s3_path: str) -> Tuple[str, str]:
    bucket_parsed = urlparse(s3_path)
    bucket_base = bucket_parsed.netloc
    folder_key = bucket_parsed.path
    folder_key = folder_key.lstrip("/")
    return bucket_base, folder_key


def s3_listdir(s3_client, s3_path: str) -> List[Dict]:
    """'ls' for s3 bucket, returns results in List"""
    bucket_base, folder_key = s3_path_to_folder_and_key(s3_path)

    objects_response = s3_client.list_objects_v2(Bucket=bucket_base, Prefix=folder_key)
    if objects_response["KeyCount"] == 0:
        return []
    dirs = []
    for key in objects_response["Contents"]:
        dirs.append(key)
    return dirs


def s3_read_to_mem(s3_client, s3_path, encoding="utf-8"):
    """Download s3 key to memory."""
    bucket_base, key = s3_path_to_folder_and_key(s3_path)
    file_obj = s3_client.get_object(Bucket=bucket_base, Key=key)
    data = file_obj["Body"].read().decode(encoding)
    return data


def fetch_mapreduce_results(s3_path: str, val_col_names: Optional[List[str]]) -> pd.DataFrame:
    """Fetches and cleans MapReduce formatted results from given s3-path."""
    session = boto3.Session(profile_name="default")
    s3 = session.client("s3")

    bucket_base, folder_key = s3_path_to_folder_and_key(s3_path)
    keys = s3_listdir(s3, s3_path)
    if not keys:
        raise ValueError(f"No files in in following S3-path: {s3_path}")

    key_names: List[str] = map(lambda d: d["Key"], keys)
    raw_data: List[str] = []
    mapr_result_prefix = os.path.join(folder_key, "part-r-")
    for key_name in key_names:
        if key_name.startswith(mapr_result_prefix):
            key_full_path = os.path.join("s3://", bucket_base, key_name)
            data = s3_read_to_mem(s3, key_full_path)
            raw_data.append(data)

    results: pd.DataFrame = clean_mapr_results(raw_data, col_names=val_col_names)
    return results
