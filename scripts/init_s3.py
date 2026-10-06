"""Create the demo bucket and copy old local artifacts without deleting originals.

Existing S3 keys are compared byte-for-byte, never silently overwritten.
"""
import hashlib
import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3", endpoint_url=os.environ["MLFLOW_S3_ENDPOINT_URL"])
bucket = os.environ["S3_BUCKET"]
try:
    s3.head_bucket(Bucket=bucket)
except ClientError as exc:
    if exc.response["Error"]["Code"] not in ("404", "NoSuchBucket"):
        raise
    s3.create_bucket(Bucket=bucket)

root = Path("/legacy-artifacts")
copied = verified = 0
for path in sorted(root.rglob("*")):
    if not path.is_file():
        continue
    key = path.relative_to(root).as_posix()
    local_sha = hashlib.sha256(path.read_bytes()).hexdigest()
    try:
        existing = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response["Error"]["Code"] not in ("404", "NoSuchKey"):
            raise
        s3.upload_file(str(path), bucket, key)
        copied += 1
        existing = s3.get_object(Bucket=bucket, Key=key)
    with existing["Body"] as stream:
        remote_sha = hashlib.sha256(stream.read()).hexdigest()
    if local_sha != remote_sha:
        raise RuntimeError(f"S3 content differs from legacy artifact: {key}")
    verified += 1
print(f"Bucket {bucket}: copied={copied}, verified={verified}. Legacy files preserved.", flush=True)
