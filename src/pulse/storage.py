"""Object-storage setup: make sure the lake bucket exists before any job writes to it."""

import logging

from pyarrow import fs

from pulse.config import Settings, get_settings

log = logging.getLogger(__name__)


def s3_filesystem(s: Settings) -> fs.S3FileSystem:
    return fs.S3FileSystem(
        endpoint_override=s.s3_endpoint,
        access_key=s.s3_access_key,
        secret_key=s.s3_secret_key,
        scheme="http" if s.s3_endpoint.startswith("http://") else "https",
        allow_bucket_creation=True,  # off by default in pyarrow, a deliberate safety switch
    )


def ensure_lake(s: Settings | None = None) -> None:
    """Idempotent: creates the bucket (or local folder) only if it is missing."""
    s = s or get_settings()
    if s.lake_backend == "local":
        s.local_lake_dir.mkdir(parents=True, exist_ok=True)
        return
    s3 = s3_filesystem(s)
    if s3.get_file_info(s.lake_bucket).type == fs.FileType.NotFound:
        s3.create_dir(s.lake_bucket)
        log.info("created bucket %s", s.lake_bucket)
