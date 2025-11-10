from typing import Optional, Tuple
from urllib.parse import urlparse


def parse_s3_url(url: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Parses an S3 URL and returns the bucket and key.
    Supports s3://bucket/key and various HTTP URL formats.
    """
    parsed_url = urlparse(url)
    bucket: Optional[str] = None
    key: Optional[str] = None

    if parsed_url.scheme == "s3":
        bucket = parsed_url.netloc or None
        key = parsed_url.path.lstrip("/") or None
    else:
        host = parsed_url.netloc
        path = parsed_url.path.lstrip("/")
        if host and ".s3." in host:
            bucket = host.split(".s3.", 1)[0] or None
            key = path or None
        if bucket is None and host and host.startswith("s3."):
            parts = path.split("/", 1)
            if parts[0]:
                bucket = parts[0]
                key = parts[1] if len(parts) > 1 else None
        if bucket is None:
            if path.startswith("s3/"):
                path = path[3:]
            elif path.startswith("storage/"):
                path = path[8:]
            path = path.lstrip("/")
            parts = path.split("/", 1)
            if parts[0]:
                bucket = parts[0]
                key = parts[1] if len(parts) > 1 else None

    if key is not None:
        key = key.lstrip("/")
        if not key:
            key = None

    return bucket, key
