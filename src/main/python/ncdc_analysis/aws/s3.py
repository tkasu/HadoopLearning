from dataclasses import dataclass
import os
from urllib.parse import urlparse
from typing import Tuple, List, Dict


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
    """'ls' for s3 bucket, returns results in List
    TODO refactor to be S3Path methods."""
    objects_response = s3_client.list_objects_v2(Bucket=path.bucket, Prefix=path.key)
    if objects_response["KeyCount"] == 0:
        return []
    dirs = []
    for key in objects_response["Contents"]:
        dirs.append(key)
    return dirs


def s3_read_to_mem(s3_client, path: S3Path, encoding="utf-8"):
    """Download s3 key to memory
    TODO TODO refactor to be S3Path methods."""
    file_obj = s3_client.get_object(Bucket=path.bucket, Key=path.key)
    data = file_obj["Body"].read().decode(encoding)
    return data
