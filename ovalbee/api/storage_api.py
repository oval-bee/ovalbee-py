from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, AsyncGenerator, Dict, List, Optional

import aioboto3
from aiobotocore.client import AioBaseClient
from botocore.config import Config
from botocore.exceptions import ClientError

try:
    import aiofiles
except ImportError:
    aiofiles = None

import logging

from ovalbee.domain.types.s3 import S3Object
from ovalbee.io.credentials import MinioCredentials, _is_ssl_url, _normalize_url
from ovalbee.io.decorators import sync_compatible, sync_compatible_generator
from ovalbee.io.env import is_development

if TYPE_CHECKING:
    from ovalbee.api.api import Api


# ----------------------------------- dataclasses -------------------------------------------
@dataclass
class StorageConfig:
    """Configuration for StorageApi client."""

    service_name: str = "s3"
    default_region: str = "us-east-1"
    addressing_style: str = "path"
    max_pool_connections: int = 10
    read_timeout: int = 60
    connect_timeout: int = 10
    max_retries: int = 5
    multipart_threshold: int = 64 * 1024 * 1024  # 64 MiB
    part_size: int = 8 * 1024 * 1024  # 8 MiB
    default_chunk_size: int = 1024 * 1024  # 1 MiB
    default_concurrency: int = 8

    def to_boto3_config(self, extra: Optional[Dict[str, Any]] = None) -> Config:
        """Convert to boto3 Config object."""
        s3_cfg = {"addressing_style": self.addressing_style}
        if extra and isinstance(extra.get(self.service_name), dict):
            s3_cfg.update(extra[self.service_name])
        return Config(
            signature_version="s3v4",
            s3=s3_cfg,
            retries={"max_attempts": self.max_retries, "mode": "standard"},
            connect_timeout=self.connect_timeout,
            read_timeout=self.read_timeout,
            max_pool_connections=self.max_pool_connections,
        )


# --------------- Bucket Operations ---------------------------------------------
class BucketOperations:
    """Bucket-related operations."""

    def __init__(self, api: _StorageApi):
        self._api = api

    @sync_compatible
    async def list(self) -> List[str]:
        """List all bucket names."""
        await self._api._ensure_connected()
        resp = await self._api._client.list_buckets()
        return [b["Name"] for b in resp.get("Buckets", [])]

    @sync_compatible
    async def exists(self, name: str) -> bool:
        """Check if bucket exists."""
        await self._api._ensure_connected()
        try:
            return True  # TODO: list buckets is not implemented yet
            client: AioBaseClient = self._api._client
            await client.head_bucket(Bucket=name)
            return True
        except ClientError as e:
            if self._is_not_found_error(e):
                return False
            raise

    @sync_compatible
    async def create(self, name: str) -> None:
        """Create a new bucket."""
        await self._api._ensure_connected()
        params = {"Bucket": name}

        if self._region and self._region != self._api._config.default_region:
            params["CreateBucketConfiguration"] = {"LocationConstraint": self._region}

        try:
            await self._api._client.create_bucket(**params)
        except Exception as e:
            if "BucketAlreadyOwnedByYou" not in str(e):
                raise

    @sync_compatible
    async def ensure(self, name: str) -> None:
        """Create bucket if it doesn't exist."""
        if not await self.exists(name):
            await self.create(name)

    @sync_compatible
    async def delete(self, name: str) -> None:
        """Delete a bucket."""
        await self._api._ensure_connected()
        await self._api._client.delete_bucket(Bucket=name)

    @staticmethod
    def _is_not_found_error(e: ClientError) -> bool:
        """Check if error indicates resource not found."""
        status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        err_code = e.response.get("Error", {}).get("Code")
        return status in (400, 403, 404) or err_code in (
            "NoSuchBucket",
            "AccessDenied",
            "InvalidBucketName",
            "InvalidRequest",
        )


# --------------- Object Operations ---------------------------------------------
class ObjectOperations:
    """Object-related operations."""

    def __init__(self, api: _StorageApi):
        self._api = api

    @sync_compatible
    async def put(
        self,
        bucket: str,
        key: str,
        body: bytes,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> str:
        """Upload object from bytes."""
        await self._api._ensure_connected()
        if self._api._on_premise and bucket != "workspace":
            logging.warning("On-premise mode: overriding bucket name to 'workspace'")
            bucket = "workspace"
        await self._api._ensure_bucket_exists(bucket)

        params = {"Bucket": bucket, "Key": key, "Body": body}
        params.update(self._build_put_params(content_type, metadata, kwargs))

        resp = await self._api._client.put_object(**params)
        return resp.get("ETag", "")

    @sync_compatible
    async def get_bytes(self, bucket: str, key: str) -> bytes:
        """Download object as bytes."""
        await self._api._ensure_connected()
        if self._api._on_premise and bucket != "workspace":
            logging.warning("On-premise mode: overriding bucket name to 'workspace'")
            bucket = "workspace"
        await self._api._ensure_bucket_exists(bucket)

        resp = await self._api._client.get_object(Bucket=bucket, Key=key)
        body = resp["Body"]
        try:
            return await body.read()
        finally:
            body.close()

    @sync_compatible
    async def get_size(self, bucket: str, key: str) -> int:
        """Get size of an object in bytes."""
        await self._api._ensure_connected()
        if self._api._on_premise and bucket != "workspace":
            logging.warning("On-premise mode: overriding bucket name to 'workspace'")
            bucket = "workspace"
        await self._api._ensure_bucket_exists(bucket)

        resp = await self._api._client.head_object(Bucket=bucket, Key=key)
        return resp["ContentLength"]

    @sync_compatible_generator
    async def stream(
        self, bucket: str, key: str, chunk_size: Optional[int] = None
    ) -> AsyncGenerator[bytes, None]:
        """Stream object content in chunks."""
        await self._api._ensure_connected()
        if self._api._on_premise and bucket != "workspace":
            logging.warning("On-premise mode: overriding bucket name to 'workspace'")
            bucket = "workspace"
        await self._api._ensure_bucket_exists(bucket)

        chunk_size = chunk_size or self._api._config.default_chunk_size
        resp = await self._api._client.get_object(Bucket=bucket, Key=key)
        body = resp["Body"]

        try:
            async for chunk in self._iter_body_chunks(body, chunk_size):
                if chunk:
                    yield chunk
        finally:
            body.close()

    @staticmethod
    def _build_put_params(
        content_type: Optional[str], metadata: Optional[Dict[str, str]], kwargs: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Build parameters for put_object call."""
        params = {}
        if content_type:
            params["ContentType"] = content_type
        if metadata:
            params["Metadata"] = metadata

        # Handle other optional parameters
        for param in ["ACL", "CacheControl", "ContentDisposition"]:
            snake_case = param.lower().replace("a", "a").replace("c", "_c")
            if snake_case in kwargs:
                params[param] = kwargs[snake_case]

        return params

    @staticmethod
    async def _iter_body_chunks(body, chunk_size: int) -> AsyncGenerator[bytes, None]:
        """Iterate over response body chunks."""
        if hasattr(body, "iter_chunks"):
            async for chunk in body.iter_chunks(chunk_size=chunk_size):
                yield chunk
        else:
            while True:
                chunk = await body.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    async def delete(self, bucket: str, key: str) -> None:
        """Delete an object."""
        await self._api._ensure_connected()
        if self._api._on_premise and bucket != "workspace":
            raise RuntimeError("Incorrect bucket for on-premise mode; must be 'workspace'")
        await self._api._ensure_bucket_exists(bucket)
        await self._api._client.delete_object(Bucket=bucket, Key=key)


# ----------------------------------------------------------------------------------
# --------------- Internal StorageApi Class ----------------------------------------
# ----------------------------------------------------------------------------------
_connection_cache: Dict[str, _StorageApi] = {}
_connection = None


class _StorageApi:
    """
    Async MinIO/S3 client wrapper built on aioboto3.
    """

    _initialized = False

    def __init__(
        self,
        api: Api,
        extra_config: Optional[Dict[str, Any]] = None,
        use_on_premise: bool = True,
    ) -> None:
        """
        Initialize StorageApi client.
        Uses connection caching to reuse clients with the same credentials.

        Args:
            api: Parent Api instance.
            extra_config: Extra configuration for boto3 Config.
            use_on_premise: Whether to use on-premise MinIO settings.
        """
        if self._initialized:
            return
        self._api = api
        self._on_premise = use_on_premise
        self._raw_config = StorageConfig()
        self._config = self._raw_config.to_boto3_config(extra=extra_config)
        self._session = aioboto3.Session()
        self._client_cm = None
        self._client = None  # type: ignore
        self._asyncio_lock = None
        self._region = None

        self.buckets = BucketOperations(self)
        self.objects = ObjectOperations(self)

        _StorageApi._register_shutdown_hooks()
        _StorageApi._initialized = True

    # --------------- Properties ---------------
    @property
    def is_connected(self) -> bool:
        """Check if client is connected."""
        return self._client is not None

    # --------------- Factory Methods ---------------
    @classmethod
    def _register_shutdown_hooks(cls) -> None:
        """Register shutdown hooks to close all cached connections."""
        try:
            import atexit
            import signal

            from ovalbee.io.decorators import _bg_run

            async def _close_connection():
                global _connection
                if _connection is not None:
                    await _connection._close()
                _connection = None

            def _sync_close_connection(*args, **kwargs):
                _bg_run(_close_connection())

            signal.signal(signal.SIGINT, _sync_close_connection)
            signal.signal(signal.SIGTERM, _sync_close_connection)
            atexit.register(_sync_close_connection)
        except Exception:
            pass

    @classmethod
    def _set_logging_levels(cls, level: int) -> None:
        """Set logging levels for aioboto3 and botocore."""
        root_logger = logging.getLogger()
        root_logger.setLevel(level)

    # --------------- Connection Management ---------------
    def __new__(cls, api: Api, **kwargs) -> _StorageApi:
        """Override __new__ to use connection cache."""
        global _connection

        if _connection is None:
            inst = super().__new__(cls)
            _connection = inst
        else:
            inst = _connection
        return inst

    async def _get_lock(self) -> asyncio.Lock:
        """Get or create the asyncio lock (lazy initialization)."""
        if self._asyncio_lock is None:
            self._asyncio_lock = asyncio.Lock()
        return self._asyncio_lock

    async def _connect(self) -> "_StorageApi":
        """Explicitly open the underlying S3 client."""
        if self.is_connected:
            return self
        lock = await self._get_lock()
        async with lock:
            current_log_level = logging.getLogger().level
            if not self._on_premise:
                creds = MinioCredentials()
                creds.validate_credentials()
                url = _normalize_url(creds.MINIO_ROOT_URL)
                use_ssl = _is_ssl_url(creds.MINIO_ROOT_URL)
                key_id = creds.MINIO_ROOT_USER.get_secret_value()
                secret_key = creds.MINIO_ROOT_PASSWORD.get_secret_value()
                self._region = creds.get_region()
            else:
                server_address = self._api.api_server_address
                # if is_development():
                #     server_address = "http://localhost:30080"
                url = _normalize_url(server_address + "/s3")
                use_ssl = False
                key_id = self._api._token
                secret_key = ""
                self._region = "us-east-1"
            _StorageApi._set_logging_levels(logging.WARNING)
            self._client_cm = self._session.client(
                service_name=self._raw_config.service_name,
                endpoint_url=url,
                aws_access_key_id=key_id,
                aws_secret_access_key=secret_key,
                use_ssl=use_ssl,
                config=self._config,
                region_name=self._region,
            )
            self._client = await self._client_cm.__aenter__()  # type: ignore
            _StorageApi._set_logging_levels(current_log_level)
        return self

    async def _close(self) -> None:
        """Explicitly close the underlying S3 client."""
        if self._client_cm is not None:
            await self._client_cm.__aexit__(None, None, None)
        self._client = None
        self._client_cm = None

    async def _ensure_connected(self) -> None:
        if self.is_connected:
            return
        await self._connect()

    async def _ensure_bucket_exists(self, bucket: str) -> None:
        if not await self.buckets.exists(bucket):
            raise RuntimeError(f"Bucket does not exist: {bucket}")


# ----------------------------------------------------------------------------------
# --------------- Main StorageApi Class ---------------------------------------------
# ----------------------------------------------------------------------------------
class StorageApi(_StorageApi):
    """
    High-level Storage API with bucket and object operations.
    Combines BucketOperations, ObjectOperations and _StorageApi functionality.
    """

    # --------------- Listing and bulk operations ---------------
    @sync_compatible_generator
    async def iter_objects(
        self,
        prefix: str = "",
        bucket: str = "workspace",
        delimiter: Optional[str] = None,
        page_size: Optional[int] = None,
        max_keys: Optional[int] = None,
        include_dirs: bool = False,
    ) -> AsyncGenerator[S3Object, None]:
        """
        Async generator yielding object summaries under a prefix.
        - delimiter="/" produces 'CommonPrefixes' (folders).
        - include_dirs=True will yield folder entries as {"Prefix": "..."} in addition to objects.
        """
        await self._ensure_connected()
        await self._ensure_bucket_exists(bucket)
        paginator = self._client.get_paginator("list_objects_v2")
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
                yield S3Object(**obj)

    @sync_compatible
    async def list_objects(
        self,
        prefix: str = "",
        bucket: str = "workspace",
        delimiter: Optional[str] = None,
        max_keys: Optional[int] = None,
        continuation_token: Optional[str] = None,
        start_after: Optional[str] = None,
        fetch_owner: bool = False,
    ) -> List[S3Object]:
        """
        Thin wrapper around S3 ListObjectsV2 API.
        Returns the raw response dict, including Contents, CommonPrefixes,
        IsTruncated, and NextContinuationToken for pagination.
        """
        await self._ensure_connected()
        await self._ensure_bucket_exists(bucket)
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

        resp = await self._client.list_objects_v2(**params)
        contents = resp.get("Contents")
        if contents is None:
            return []
        return [S3Object(**obj) for obj in contents]

    # --------------- File helpers (local disk <-> S3) ---------------
    @sync_compatible
    async def upload(
        self,
        file_path: str,
        key: str,
        bucket: str = "workspace",
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        multipart_threshold: int = 64 * 1024 * 1024,  # 64 MiB
        part_size: int = 8 * 1024 * 1024,  # 8 MiB
    ) -> str:
        """
        Uploads a local file. Uses single PUT for small files; multipart for large files.
        Returns ETag (for multipart, the ETag is not a simple MD5).
        """
        await self._ensure_connected()
        await self._ensure_bucket_exists(bucket)
        if aiofiles is None:
            with open(file_path, "rb") as f:
                data = f.read()
            return await self.objects.put(
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
            return await self.objects.put(
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

        resp = await self._client.create_multipart_upload(**create_params)
        upload_id = resp["UploadId"]
        parts: List[Dict[str, Any]] = []
        part_number = 1

        try:
            async with aiofiles.open(file_path, "rb") as f:
                while chunk := await f.read(part_size):
                    up = await self._client.upload_part(
                        Bucket=bucket,
                        Key=key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=chunk,
                    )
                    parts.append({"PartNumber": part_number, "ETag": up["ETag"]})
                    part_number += 1

            complete = await self._client.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            return complete.get("ETag", "")
        except Exception:
            try:
                await self._client.abort_multipart_upload(
                    Bucket=bucket, Key=key, UploadId=upload_id
                )
            except Exception:
                pass
            raise

    @sync_compatible
    async def upload_dir(
        self,
        dir_path: str,
        prefix: str = "",
        bucket: str = "workspace",
        *,
        concurrency: int = 8,
        include_hidden: bool = False,
        follow_symlinks: bool = False,
    ) -> List[str]:
        """
        Uploads all files under 'dir_path' into bucket/prefix.
        Returns list of uploaded S3 keys.
        Args:
            dir_path: Local directory path to upload.
            prefix: S3 key prefix under which to upload files.
            bucket: Target S3 bucket name.
            concurrency: Number of concurrent uploads.
            include_hidden: Whether to include hidden files and directories.
            follow_symlinks: Whether to follow symlinks.
        Returns:
            List of uploaded S3 keys.
        """
        await self._ensure_connected()
        await self._ensure_bucket_exists(bucket)
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
                await self.upload(str(p), bucket=bucket, key=key)
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

        for res in await asyncio.gather(*tasks):
            uploaded.append(res)
        return uploaded

    @sync_compatible
    async def download(
        self,
        key: str,
        file_path: str,
        bucket: str = "workspace",
        make_dirs: bool = True,
    ) -> None:
        """
        Downloads an object to a local file, streaming to disk.
        """
        await self._ensure_connected()
        await self._ensure_bucket_exists(bucket)
        if make_dirs:
            os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)

        resp = await self._client.get_object(Bucket=bucket, Key=key)
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
        prefix: str,
        dest_dir: str,
        bucket: str = "workspace",
        *,
        concurrency: int = 8,
        make_dirs: bool = True,
        skip_empty_dir_markers: bool = True,
    ) -> List[str]:
        """
        Downloads all objects under 'prefix' into 'dest_dir'.
        Returns list of local file paths written.
        """
        await self._ensure_connected()
        await self._ensure_bucket_exists(bucket)
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
                await self.download(bucket, key, str(local_path), make_dirs=make_dirs)
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
        key: str,
        bucket: str = "workspace",
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

        return self._client.generate_presigned_url(
            "get_object", Params=params, ExpiresIn=expires_in, HttpMethod="GET"
        )
