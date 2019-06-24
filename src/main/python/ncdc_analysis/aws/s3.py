from dataclasses import dataclass
import boto3
import os
from urllib.parse import urlparse
import pandas as pd
from typing import Tuple, List, Dict, Optional, Union
from ncdc_analysis.postprocessing.map_reduce_utils import clean_mapr_results
from ncdc_analysis.postprocessing.spark_utils import clean_spark_results


@dataclass
class S3Path:
    bucket: str
    key: str

    @property
    def path(self) -> str:
        return os.path.join("s3://", self.bucket, self.key)

    def join(self, key_extension: str):
        new_path = os.path.join(self.path, key_extension)
        return self.from_path(new_path)

    @staticmethod
    def _s3_path_to_folder_and_key(s3_path: str) -> Tuple[str, str]:
        bucket_parsed = urlparse(s3_path)
        bucket_base = bucket_parsed.netloc
        folder_key = bucket_parsed.path
        folder_key = folder_key.lstrip("/")
        return bucket_base, folder_key

    @classmethod
    def from_path(cls, path: str):
        bucket, key = cls._s3_path_to_folder_and_key(path)
        return cls(bucket, key)


def s3_listdir(s3_client, path: S3Path) -> List[Dict]:
    """'ls' for s3 bucket, returns results in List"""
    objects_response = s3_client.list_objects_v2(Bucket=path.bucket, Prefix=path.key)
    if objects_response["KeyCount"] == 0:
        return []
    dirs = []
    for key in objects_response["Contents"]:
        dirs.append(key)
    return dirs


def s3_read_to_mem(s3_client, path: S3Path, encoding="utf-8"):
    """Download s3 key to memory."""
    file_obj = s3_client.get_object(Bucket=path.bucket, Key=path.key)
    data = file_obj["Body"].read().decode(encoding)
    return data


def fetch_hadoop_style_results(path: S3Path, col_names: Union[bool, Optional[List[str]]],
                               spark: bool = False) -> pd.DataFrame:
    """Fetches and cleans MapReduce formatted results from given s3-path.
    col_names behaves as following:
      True == column names in the first row
      None == generates int column names from index 0
      List[str] == Uses these as column names"""
    session = boto3.Session(profile_name="default")
    s3 = session.client("s3")

    keys = s3_listdir(s3, path)
    if not keys:
        raise ValueError(f"No files in in following S3-path: {path.path}")

    key_names: List[str] = map(lambda d: d["Key"], keys)
    raw_data: List[str] = []
    result_prefix: S3Path = path.join("part-")
    for key_name in key_names:
        if key_name.startswith(result_prefix.key):
            key_full_path: S3Path = S3Path(bucket=path.bucket, key=key_name)
            data = s3_read_to_mem(s3, key_full_path)
            raw_data.append(data)

    if spark:
        results: pd.DataFrame = clean_spark_results(raw_data)
    else:
        results: pd.DataFrame = clean_mapr_results(raw_data, col_names=col_names)
    return results
