from abc import ABCMeta, abstractmethod
import boto3
from typing import List, Optional, Union
import pandas as pd

from ncdc_analysis.aws.s3 import S3Path, s3_listdir, s3_read_to_mem
from ..postprocessing.map_reduce_utils import clean_mapr_results
from ..postprocessing.spark_utils import clean_spark_results


class EMRResultFetcher(metaclass=ABCMeta):

    @staticmethod
    def _fetch_hadoop_style_results(path: S3Path, col_names: Union[bool, Optional[List[str]]],
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

    @abstractmethod
    def fetch(self, path: S3Path):
        pass


class EMRResultCsvFetcher(EMRResultFetcher):
    col_names: Optional[List[str]] = None
    output_path: str  # os.path.join(LOCAL_OUTPUT_PATH, f"{run_timestamp}_ncdc_emr_results.csv")
    spark: bool = False

    def fetch(self, path: S3Path):
        result_df = self._fetch_hadoop_style_results(path=path, col_names=self.col_names, spark=self.spark)
        result_df.to_csv(self.output_path)
