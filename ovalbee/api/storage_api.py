from __future__ import annotations

import asyncio
import functools
import os
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional

import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError

try:
    import aiofiles
except ImportError:
    aiofiles = None


def sync_compatible(async_fn):
    @functools.wraps(async_fn)
    def wrapper(self, *args, **kwargs):
        coro = async_fn(self, *args, **kwargs)
        try:
            asyncio.get_running_loop()
            # We're in an async context, return the coroutine
            return coro
        except RuntimeError:
            return asyncio.run(coro)

    return wrapper


class StorageApi:
    """
    Async MinIO/S3 client wrapper built on aioboto3.
    """

    DEFAULT_ENV_PATH = "~/.ovalbee/storage_env"

    def __init__(
        self,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        region_name: Optional[str] = None,
        use_ssl: bool = True,
        addressing_style: str = "path",
        max_pool_connections: int = 10,
        read_timeout: int = 60,
        connect_timeout: int = 10,
        extra_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.use_ssl = use_ssl
        self.region_name = (
            region_name
            or os.getenv("MINIO_REGION")
            or os.getenv("AWS_REGION")
            or os.getenv("AWS_DEFAULT_REGION")
            or "us-east-1"
        )

        s3_cfg = {"addressing_style": addressing_style}
        if extra_config and isinstance(extra_config.get("s3"), dict):
            s3_cfg.update(extra_config["s3"])

        self._config = Config(
            signature_version="s3v4",
            s3=s3_cfg,
            retries={"max_attempts": 5, "mode": "standard"},
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            max_pool_connections=max_pool_connections,
        )

        self._session = aioboto3.Session()
        self._client_cm = None
        self.client = None  # type: ignore

    @classmethod
    def from_env(cls) -> StorageApi:
        """
        Creates a StorageApi instance using environment variables.
        """
        from dotenv import load_dotenv

        load_dotenv(os.path.expanduser(cls.DEFAULT_ENV_PATH))
        raw = os.getenv("MINIO_ROOT_URL", "http://localhost:9000")
        if "://" not in raw:
            endpoint_url = f"http://{raw}"
            use_ssl = False
        else:
            endpoint_url = raw
            use_ssl = raw.startswith("https://")

        access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
        secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

        return cls(
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
            use_ssl=use_ssl,
        )

    async def __aenter__(self) -> StorageApi:
        self._client_cm = self._session.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            use_ssl=self.use_ssl,
            config=self._config,
        )
        self.client = await self._client_cm.__aenter__()  # type: ignore
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    def __enter__(self) -> StorageApi:
        """Sync context manager entry. Use connect() first in sync code."""
        if self.client is None:
            asyncio.run(self.connect())
        return self

    def __exit__(self, exc_type, exc, tb):
        """Sync context manager exit."""
        try:
            asyncio.run(self.close())
        except RuntimeError:
            pass

    async def connect(self) -> "StorageApi":
        """Explicitly open the underlying S3 client."""
        if self.client is not None:
            return self
        self._client_cm = self._session.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            use_ssl=self.use_ssl,
            config=self._config,
        )
        self.client = await self._client_cm.__aenter__()  # type: ignore
        return self

    async def close(self) -> None:
        """Explicitly close the underlying S3 client."""
        if self._client_cm is not None:
            await self._client_cm.__aexit__(None, None, None)
        self.client = None
        self._client_cm = None

    def _assert_client(self) -> None:
        if self.client is None:
            raise RuntimeError(
                "StorageApi is not connected. Use 'with StorageApi(...) as s:' or call 's.connect()' before using it."
            )

    async def _validate_bucket_existance(self, bucket: str) -> None:
        if not await self.bucket_exists(bucket):
            raise RuntimeError(f"Bucket does not exist: {bucket}")

    # --------------- Bucket management ---------------

    @sync_compatible
    async def list_buckets(self) -> List[str]:
        """
        Returns a list of bucket names.
        """
        self._assert_client()
        resp = await self.client.list_buckets()
        return [b["Name"] for b in resp.get("Buckets", [])]

    @sync_compatible
    async def bucket_exists(self, bucket: str) -> bool:
        """
        Checks if a bucket exists.
        """
        self._assert_client()
        try:
            await self.client.head_bucket(Bucket=bucket)
            return True
        except ClientError as e:
            status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            err_code = e.response.get("Error", {}).get("Code")
            if status in (400, 403, 404) or err_code in (
                "NoSuchBucket",
                "AccessDenied",
                "InvalidBucketName",
                "InvalidRequest",
            ):
                return False
            raise

    @sync_compatible
    async def create_bucket(self, bucket: str) -> None:
        """
        Creates a bucket.
        """
        self._assert_client()
        params: Dict[str, Any] = {"Bucket": bucket}
        if self.region_name and self.region_name != "us-east-1":
            params["CreateBucketConfiguration"] = {
                "LocationConstraint": self.region_name
            }
        try:
            await self.client.create_bucket(**params)
        except Exception as e:
            if "BucketAlreadyOwnedByYou" in e.args[0]:
                return
            raise

    @sync_compatible
    async def ensure_bucket(self, bucket: str) -> None:
        """
        Create the bucket if it does not exist.
        """
        self._assert_client()
        if not await self.bucket_exists(bucket):
            await self.create_bucket(bucket)

    @sync_compatible
    async def delete_bucket(self, bucket: str) -> None:
        """
        Deletes a bucket.
        """
        self._assert_client()
        await self.client.delete_bucket(Bucket=bucket)

    # --------------- Object CRUD ---------------

    @sync_compatible
    async def put_object(
        self,
        bucket: str,
        key: str,
        body: bytes,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        acl: Optional[str] = None,
        cache_control: Optional[str] = None,
        content_disposition: Optional[str] = None,
    ) -> str:
        """
        Uploads a bytes payload as an object. Returns ETag.
        """
        self._assert_client()
        params: Dict[str, Any] = dict(Bucket=bucket, Key=key, Body=body)
        if content_type:
            params["ContentType"] = content_type
        if metadata:
            params["Metadata"] = metadata
        if acl:
            params["ACL"] = acl
        if cache_control:
            params["CacheControl"] = cache_control
        if content_disposition:
            params["ContentDisposition"] = content_disposition

        resp = await self.client.put_object(**params)
        return resp.get("ETag", "")

    @sync_compatible
    async def get_object_bytes(self, bucket: str, key: str) -> bytes:
        """
        Downloads an object and returns its full bytes. Suitable for small/medium objects.
        """
        self._assert_client()
        resp = await self.client.get_object(Bucket=bucket, Key=key)
        body = resp["Body"]
        data = await body.read()
        body.close()
        return data

    @sync_compatible
    async def stream_object(
        self, bucket: str, key: str, chunk_size: int = 1024 * 1024
    ) -> AsyncGenerator[bytes, None]:
        """
        Streams an object's bytes in chunks. Yields bytes. Caller consumes and writes to destination.
        """
        self._assert_client()
        resp = await self.client.get_object(Bucket=bucket, Key=key)
        body = resp["Body"]
        try:
            if hasattr(body, "iter_chunks"):
                async for chunk in body.iter_chunks(chunk_size=chunk_size):
                    if chunk:
                        yield chunk
            else:
                while True:
                    chunk = await body.read(chunk_size)
                    if not chunk:
                        break
                    yield chunk
        finally:
            body.close()

    @sync_compatible
    async def get_object_metadata(self, bucket: str, key: str) -> Dict[str, Any]:
        """
        Returns object metadata (size, ETag, last-modified, etc.).
        """
        self._assert_client()
        try:
            head = await self.client.head_object(Bucket=bucket, Key=key)
            return head.get("Metadata", {})
        except ClientError as e:
            return {}

    @sync_compatible
    async def object_exists(self, bucket: str, key: str) -> bool:
        """
        Returns True if the object exists.
        """
        self._assert_client()
        try:
            await self.client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            return False

    @sync_compatible
    async def delete_object(self, bucket: str, key: str) -> None:
        """
        Deletes a single object.
        """
        self._assert_client()
        await self.client.delete_object(Bucket=bucket, Key=key)

    async def copy_object(
        self,
        src_bucket: str,
        src_key: str,
        dst_bucket: str,
        dst_key: str,
        metadata: Optional[Dict[str, str]] = None,
        metadata_directive: str = "COPY",
        acl: Optional[str] = None,
        content_type: Optional[str] = None,
    ) -> str:
        """
        Server-side copy. Returns ETag of the destination.
        """
        self._assert_client()
        params: Dict[str, Any] = {
            "Bucket": dst_bucket,
            "Key": dst_key,
            "CopySource": {"Bucket": src_bucket, "Key": src_key},
            "MetadataDirective": metadata_directive,
        }
        if metadata and metadata_directive == "REPLACE":
            params["Metadata"] = metadata
        if acl:
            params["ACL"] = acl
        if content_type:
            params["ContentType"] = content_type

        resp = await self.client.copy_object(**params)
        return resp.get("CopyObjectResult", {}).get("ETag", "")

    # --------------- Listing and bulk operations ---------------

    @sync_compatible
    async def iter_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: Optional[str] = None,
        page_size: Optional[int] = None,
        max_keys: Optional[int] = None,
        include_dirs: bool = False,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Async generator yielding object summaries under a prefix.
        - delimiter="/" produces 'CommonPrefixes' (folders).
        - include_dirs=True will yield folder entries as {"Prefix": "..."} in addition to objects.
        """
        self._assert_client()
        await self._validate_bucket_existance(bucket)
        paginator = self.client.get_paginator("list_objects_v2")
        paginate_params: Dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if delimiter is not None:
            paginate_params["Delimiter"] = delimiter
        if page_size:
            paginate_params["PaginationConfig"] = {"PageSize": page_size}
        if max_keys:
            paginate_params["MaxKeys"] = max_keys

        async for page in paginator.paginate(**paginate_params):
            if include_dirs and "CommonPrefixes" in page:
                for d in page["CommonPrefixes"]:
                    yield {"Prefix": d.get("Prefix")}
            for obj in page.get("Contents", []):
                yield obj

    @sync_compatible
    async def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: Optional[str] = None,
        max_keys: Optional[int] = None,
        continuation_token: Optional[str] = None,
        start_after: Optional[str] = None,
        fetch_owner: bool = False,
    ) -> Dict[str, Any]:
        """
        Thin wrapper around S3 ListObjectsV2 API.
        Returns the raw response dict, including Contents, CommonPrefixes,
        IsTruncated, and NextContinuationToken for pagination.
        """
        self._assert_client()
        await self._validate_bucket_existance(bucket)
        params: Dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if delimiter is not None:
            params["Delimiter"] = delimiter
        if max_keys is not None:
            params["MaxKeys"] = max_keys
        if continuation_token:
            params["ContinuationToken"] = continuation_token
        if start_after:
            params["StartAfter"] = start_after
        if fetch_owner:
            params["FetchOwner"] = True

        resp = await self.client.list_objects_v2(**params)
        return resp.get("Contents", [])

    # --------------- File helpers (local disk <-> S3) ---------------

    @sync_compatible
    async def upload_file(
        self,
        file_path: str,
        bucket: str,
        key: str,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        multipart_threshold: int = 64 * 1024 * 1024,  # 64 MiB
        part_size: int = 8 * 1024 * 1024,  # 8 MiB
    ) -> str:
        """
        Uploads a local file. Uses single PUT for small files; multipart for large files.
        Returns ETag (for multipart, the ETag is not a simple MD5).
        """
        self._assert_client()
        await self._validate_bucket_existance(bucket)
        if aiofiles is None:
            with open(file_path, "rb") as f:
                data = f.read()
            return await self.put_object(
                bucket=bucket,
                key=key,
                body=data,
                content_type=content_type,
                metadata=metadata,
            )

        size = os.path.getsize(file_path)
        if size < multipart_threshold:
            async with aiofiles.open(file_path, "rb") as f:
                data = await f.read()
            return await self.put_object(
                bucket=bucket,
                key=key,
                body=data,
                content_type=content_type,
                metadata=metadata,
            )

        create_params: Dict[str, Any] = {"Bucket": bucket, "Key": key}
        if content_type:
            create_params["ContentType"] = content_type
        if metadata:
            create_params["Metadata"] = metadata

        resp = await self.client.create_multipart_upload(**create_params)
        upload_id = resp["UploadId"]
        parts: List[Dict[str, Any]] = []
        part_number = 1

        try:
            async with aiofiles.open(file_path, "rb") as f:
                while chunk := await f.read(part_size):
                    up = await self.client.upload_part(
                        Bucket=bucket,
                        Key=key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=chunk,
                    )
                    parts.append({"PartNumber": part_number, "ETag": up["ETag"]})
                    part_number += 1

            complete = await self.client.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            return complete.get("ETag", "")
        except Exception:
            try:
                await self.client.abort_multipart_upload(
                    Bucket=bucket, Key=key, UploadId=upload_id
                )
            except Exception:
                pass
            raise

    @sync_compatible
    async def upload_dir(
        self,
        dir_path: str,
        bucket: str,
        prefix: str = "",
        *,
        concurrency: int = 8,
        include_hidden: bool = True,
        follow_symlinks: bool = False,
    ) -> List[str]:
        """
        Uploads all files under 'dir_path' into bucket/prefix.
        Returns list of uploaded S3 keys.
        """
        self._assert_client()
        await self.ensure_bucket(bucket)
        root = Path(dir_path).expanduser().resolve()
        if not root.is_dir():
            raise ValueError(f"Not a directory: {dir_path}")

        uploaded: List[str] = []
        sem = asyncio.Semaphore(concurrency)

        key_prefix = prefix.strip("/")

        def to_key(p: Path) -> str:
            rel_posix = p.relative_to(root).as_posix()
            return f"{key_prefix}/{rel_posix}" if key_prefix else rel_posix

        async def _one(p: Path) -> str:
            key = to_key(p)
            async with sem:
                await self.upload_file(str(p), bucket=bucket, key=key)
            return key

        tasks: List[asyncio.Task] = []
        for dirpath, dirnames, filenames in os.walk(root, followlinks=follow_symlinks):
            dpath = Path(dirpath)
            if not include_hidden:
                dirnames[:] = [d for d in dirnames if not d.startswith(".")]
                filenames = [f for f in filenames if not f.startswith(".")]

            for fname in filenames:
                fpath = dpath / fname
                if not follow_symlinks and fpath.is_symlink():
                    continue
                tasks.append(asyncio.create_task(_one(fpath)))

        for res in await asyncio.gather(*tasks, return_exceptions=True):
            if isinstance(res, Exception):
                raise res
            uploaded.append(res)
        return uploaded

    @sync_compatible
    async def download_file(
        self, bucket: str, key: str, file_path: str, make_dirs: bool = True
    ) -> None:
        """
        Downloads an object to a local file, streaming to disk.
        """
        self._assert_client()
        await self._validate_bucket_existance(bucket)
        if make_dirs:
            os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)

        resp = await self.client.get_object(Bucket=bucket, Key=key)
        body = resp["Body"]

        if aiofiles is None:
            data = await body.read()
            body.close()
            with open(file_path, "wb") as f:
                f.write(data)
            return

        try:
            async with aiofiles.open(file_path, "wb") as f:
                if hasattr(body, "iter_chunks"):
                    async for chunk in body.iter_chunks():
                        if chunk:
                            await f.write(chunk)
                else:
                    while True:
                        chunk = await body.read(1024 * 1024)
                        if not chunk:
                            break
                        await f.write(chunk)
        finally:
            body.close()

    @sync_compatible
    async def download_dir(
        self,
        bucket: str,
        prefix: str,
        dest_dir: str,
        *,
        concurrency: int = 8,
        make_dirs: bool = True,
        skip_empty_dir_markers: bool = True,
    ) -> List[str]:
        """
        Downloads all objects under 'prefix' into 'dest_dir'.
        Returns list of local file paths written.
        """
        self._assert_client()
        await self._validate_bucket_existance(bucket)
        dest_root = Path(dest_dir)
        written: List[str] = []
        sem = asyncio.Semaphore(concurrency)

        norm_prefix = prefix.lstrip("/")

        async def _one(key: str) -> Optional[str]:
            if skip_empty_dir_markers and key.endswith("/"):
                return None
            rel = key[len(norm_prefix) :].lstrip("/") if norm_prefix else key
            local_path = dest_root / rel
            async with sem:
                await self.download_file(
                    bucket, key, str(local_path), make_dirs=make_dirs
                )
            return str(local_path)

        tasks: List[asyncio.Task] = []
        async for obj in self.iter_objects(bucket=bucket, prefix=norm_prefix):
            key = obj.get("Key")
            if not key:
                continue
            tasks.append(asyncio.create_task(_one(key)))

        for res in await asyncio.gather(*tasks, return_exceptions=True):
            if isinstance(res, Exception):
                raise res
            if res:
                written.append(res)
        return written

    def generate_download_url(
        self,
        bucket: str,
        key: str,
        expires_in: int = 3600,
        response_content_type: Optional[str] = None,
        response_content_disposition: Optional[str] = None,
    ) -> str:
        """
        Generates a presigned GET URL for downloading an object.
        Note: This is a synchronous method as it doesn't require S3 communication.
        """
        params: Dict[str, Any] = {"Bucket": bucket, "Key": key}
        if response_content_type:
            params["ResponseContentType"] = response_content_type
        if response_content_disposition:
            params["ResponseContentDisposition"] = response_content_disposition

        return self.client.generate_presigned_url(
            "get_object", Params=params, ExpiresIn=expires_in, HttpMethod="GET"
        )
