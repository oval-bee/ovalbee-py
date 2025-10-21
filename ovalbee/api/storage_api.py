import asyncio
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import AsyncIterator, Awaitable, Callable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

from aioboto3 import Session
from boto3 import client as boto3_client
from botocore.exceptions import ClientError
from minio import Minio
from types_aiobotocore_s3.client import S3Client

from ovalbee.io.fs import iter_files


async def process_queue(
    items_iterator: AsyncIterator,
    worker_func: Callable[[any], Awaitable[None]],
    concurrency: int = 100,
) -> None:
    """
    Process many items concurrently with limited concurrency.
    Cancels all pending tasks if any worker fails.
    """

    queue = asyncio.Queue(maxsize=concurrency * 2)
    items_exhausted = asyncio.Event()
    stop_event = asyncio.Event()

    async def producer():
        try:
            async for item in items_iterator:
                if stop_event.is_set():
                    break
                await queue.put(item)
        except Exception as e:
            stop_event.set()
            raise e
        finally:
            items_exhausted.set()

    async def worker():
        while not stop_event.is_set():
            try:
                item = await asyncio.wait_for(queue.get(), timeout=0.2)
            except asyncio.TimeoutError:
                if items_exhausted.is_set() and queue.empty():
                    return
                continue

            try:
                await worker_func(item)
            except Exception as e:
                stop_event.set()
                raise e
            finally:
                queue.task_done()

    producer_task = asyncio.create_task(producer())
    workers = [asyncio.create_task(worker()) for _ in range(concurrency)]

    results = await asyncio.gather(producer_task, *workers, return_exceptions=True)
    for r in results:
        if isinstance(r, Exception):
            raise r


@dataclass
class S3Object:
    """S3 object metadata"""

    key: str
    size: int
    last_modified: datetime
    etag: str
    storage_class: Optional[str] = None

    @classmethod
    def from_s3_response(cls, obj: dict) -> "S3Object":
        """
        Create S3Object from S3 API response.

        Args:
            obj: Dictionary from S3 list_objects_v2 response

        Returns:
            S3Object instance with parsed metadata
        """
        return cls(
            key=obj["Key"],
            size=obj["Size"],
            last_modified=obj["LastModified"],
            etag=obj["ETag"].strip('"'),
            storage_class=obj.get("StorageClass"),
        )


# TODO: Remove region and storage_url from methods? It is not going to be used by most of the users.
# TODO: Reuse session.client for all the downloads/uploads in download_dir and upload_dir
class S3StorageClient:
    """
    Async S3-compatible storage client.
    Supports AWS S3, MinIO, and other S3-compatible storage services.
    Provides concurrent operations with optional rate limiting via semaphore.

    Args:
        access_key: Access key for authentication
        secret_key: Secret key for authentication
        region: AWS region (default: None, uses service default)
        storage_url: Custom endpoint URL (required for MinIO/non-AWS, None for AWS S3)
        semaphore: Optional asyncio.Semaphore for rate limiting (default: None, unlimited)

    Example:
        # MinIO
        client = StorageClient(
            access_key="your-minio-username",
            secret_key="your-minio-password",
            storage_url="your-minio-storage-url",
        )

        # AWS S3
        client = StorageClient(
            access_key="your-aws-access-key",
            secret_key="your-aws-secret-access-key",
            region="us-east-1"
        )
    """

    def __init__(
        self,
        access_key: str,
        secret_key: str,
        region: Optional[str] = None,
        storage_url: Optional[str] = None,
        semaphore: Optional[asyncio.Semaphore] = None,
    ):
        self.region = region
        self.storage_url = storage_url
        self.session = Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        self.semaphore = semaphore
        self._clients_cache = {}

    @asynccontextmanager
    async def _maybe_with_semaphore(self) -> AsyncIterator[None]:
        """Context manager that acquires semaphore if it exists, otherwise does nothing."""
        if self.semaphore:
            async with self.semaphore:
                yield
        else:
            yield

    async def get_client(
        self,
        region: Optional[str] = None,
        storage_url: Optional[str] = None,
    ) -> S3Client:
        """
        Get an S3 client instance.

        Args:
            region: Override default region for this client
            storage_url: Override default storage URL for this client

        Returns:
            Configured S3 client
        """
        if storage_url is None:
            storage_url = self.storage_url
        if region is None:
            region = self.region
        region_url_pair = (region, storage_url)
        if region_url_pair not in self._clients_cache:
            self._clients_cache[region_url_pair] = self.session.client(
                "s3", region_name=region, endpoint_url=storage_url
            )
        return self._clients_cache[region_url_pair]

    async def iterate_objects(
        self,
        bucket: str,
        prefix: str = "",
        region: Optional[str] = None,
        storage_url: Optional[str] = None,
    ) -> AsyncIterator[S3Object]:
        """
        Async generator that yields S3 objects one at a time.

        Memory-efficient for large buckets as it streams results without
        loading all objects into memory.

        Args:
            bucket: Bucket name
            prefix: Prefix to filter objects (e.g., "documents/reports/")
            region: Override default region
            storage_url: Override default storage URL

        Yields:
            S3Object instances for each object found

        Example:
            async for obj in client.iterate_objects("my-bucket", prefix="data/"):
                print(f"{obj.key}: {obj.size} bytes")
        """
        async with await self.get_client(storage_url=storage_url, region=region) as s3:
            paginator = s3.get_paginator("list_objects_v2")
            async for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                if "Contents" not in page:
                    continue
                for obj in page["Contents"]:
                    yield S3Object.from_s3_response(obj)

    async def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        region: Optional[str] = None,
        storage_url: Optional[str] = None,
    ) -> List[S3Object]:
        """
        List all objects with given prefix.

        Loads all objects into memory. For large buckets, consider using
        iterate_objects() instead for better memory efficiency.

        Args:
            bucket: Bucket name
            prefix: Prefix to filter objects (e.g., "documents/reports/")
            region: Override default region
            storage_url: Override default storage URL

        Returns:
            List of S3Object instances

        Example:
            objects = await client.list_objects("my-bucket", prefix="data/2024/")
            for obj in objects:
                print(f"{obj.key}: {obj.size} bytes")
        """
        objects = []
        async for obj in self.iterate_objects(
            bucket=bucket, prefix=prefix, region=region, storage_url=storage_url
        ):
            objects.append(obj)
        return objects

    async def get_file_size(
        self, bucket: str, key: str, region: Optional[str] = None, storage_url: Optional[str] = None
    ) -> int:
        """
        Get the size of an object in bytes.

        Args:
            bucket: Bucket name
            key: Object key (path in bucket)
            region: Override default region
            storage_url: Override default storage URL

        Returns:
            File size in bytes

        Example:
            size = await client.get_file_size("my-bucket", "data/file.zip")
            print(f"File size: {size / (1024**2):.2f} MB")
        """
        async with await self.get_client(storage_url=storage_url, region=region) as s3:
            response = await s3.head_object(Bucket=bucket, Key=key)
            file_size = response["ContentLength"]
            return file_size

    async def download(
        self,
        bucket: str,
        key: str,
        local_path: str,
        region: Optional[str] = None,
        storage_url: Optional[str] = None,
        progress_cb: Optional[Callable[[int], None]] = None,
        _session_client: Optional[S3Client] = None,
    ) -> None:
        """
        Download a file from storage.

        Respects max_concurrency limit set during initialization.
        Creates parent directories automatically if they don't exist.

        Args:
            bucket: Bucket name
            key: Object key (path in bucket)
            local_path: Local file path to save to
            region: Override default region
            storage_url: Override default storage URL
            progress_cb: Optional callback function called with bytes downloaded per chunk

        Example:
            # Simple download
            await client.download("my-bucket", "collections/collection1/image_1.png", "./data/image_1.png")

            # With progress tracking
            from tqdm import tqdm

            size = await client.get_file_size("my-bucket", "datasets/dataset1.zip")
            with tqdm(total=size, unit='B', unit_scale=True) as pbar:
                await client.download(
                    bucket="my-bucket",
                    key="datasets/dataset1.zip",
                    local_path="./data/dataset.zip",
                    progress_cb=pbar.update
                )
        """
        async with self._maybe_with_semaphore():
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            async with await self.get_client(storage_url=storage_url, region=region) as s3:
                if progress_cb:
                    with open(local_path, "wb") as f:
                        await s3.download_fileobj(bucket, key, f, Callback=progress_cb)
                else:
                    await s3.download_file(bucket, key, local_path)

    async def download_dir(
        self,
        bucket: str,
        dir_key_prefix: str,
        local_path: str,
        region: Optional[str] = None,
        storage_url: Optional[str] = None,
        progress_cb: Optional[Callable[[int], None]] = None,
        skip_existing_files: Optional[bool] = False,
    ) -> None:
        """
        Download all objects with given prefix to local directory, preserving structure.

        Downloads files concurrently with automatic rate limiting if max_concurrency is set.
        Directory structure under the prefix is preserved in the local filesystem.

        Args:
            bucket: Bucket name
            dir_key_prefix: Prefix for objects to download (e.g., "documents/reports/")
            local_path: Local directory path to save files
            region: Override default region
            storage_url: Override default storage URL
            progress_cb: Optional progress callback called for each downloaded chunk
            skip_existing_files: Skip existing files. Will raise FileExistsError if not set

        Example:
            # Download entire directory
            await client.download_dir(
                bucket="my-bucket",
                dir_key_prefix="collections/",
                local_path="./data"
            )
            # Creates: ./data/collection1/img_1.png, ./data/collection1/img_2.png, etc.

            # With progress tracking for each file
            from tqdm import tqdm

            with tqdm(unit='B', unit_scale=True, desc="Downloading") as pbar:
                await client.download_dir(
                    bucket="my-bucket",
                    dir_key_prefix="collections/",
                    local_path="./data",
                    progress_cb=pbar.update
                )
        """
        if dir_key_prefix and dir_key_prefix[-1] != "/":
            dir_key_prefix = dir_key_prefix + "/"
        dir_path = Path(local_path)

        async def _items_iterator():
            async for obj in self.iterate_objects(
                bucket=bucket, prefix=dir_key_prefix, region=region, storage_url=storage_url
            ):
                if obj.key.endswith("/"):
                    continue
                relative_path = obj.key[len(dir_key_prefix) :].lstrip("/")
                file_path = dir_path / relative_path
                if file_path.exists():
                    if skip_existing_files:
                        if progress_cb is not None:
                            progress_cb(obj.size)
                        continue
                    else:
                        raise FileExistsError(f"File '{file_path}' already exists")
                yield obj.key

        async def _worker_function(obj_key: str):
            if obj_key.endswith("/"):
                return
            relative_path = obj_key[len(dir_key_prefix) :].lstrip("/")
            file_path = dir_path / relative_path
            file_path.parent.mkdir(parents=True, exist_ok=True)
            await self.download(
                bucket=bucket,
                key=obj_key,
                local_path=file_path,
                region=region,
                storage_url=storage_url,
                progress_cb=progress_cb,
            )

        await process_queue(
            _items_iterator(),
            _worker_function,
            concurrency=100,  # 100 simultaneously opened files
        )

    async def upload(
        self,
        file_path: str,
        bucket: str,
        key: str,
        region: Optional[str] = None,
        storage_url: Optional[str] = None,
        progress_cb: Optional[Callable[[int], None]] = None,
    ) -> None:
        """
        Upload a file to storage.

        Respects max_concurrency limit set during initialization.

        Args:
            file_path: Local file path to upload
            bucket: Bucket name
            key: Object key (path in bucket) where file will be stored
            region: Override default region
            storage_url: Override default storage URL
            progress_cb: Optional callback function called with bytes uploaded per chunk

        Example:
            # Simple upload
            await client.upload(
                file_path="./data/image_1.png",
                bucket="my-bucket",
                key="collections/collection1/image_1.png"
            )

            # With progress tracking
            from tqdm import tqdm

            file_size = Path("./data/dataset.zip").stat().st_size
            with tqdm(total=file_size, unit='B', unit_scale=True, desc="Uploading") as pbar:
                await client.upload(
                    file_path="./data/dataset.zip",
                    bucket="my-bucket",
                    key="datasets/dataset1.zip",
                    progress_cb=pbar.update
                )
        """
        async with self._maybe_with_semaphore():
            async with await self.get_client(storage_url=storage_url, region=region) as s3:
                if progress_cb:
                    with open(file_path, "rb") as f:
                        await s3.upload_fileobj(f, bucket, key, Callback=progress_cb)
                else:
                    await s3.upload_file(file_path, bucket, key)

    async def upload_dir(
        self,
        dir_path: str,
        bucket: str,
        dir_key_prefix: str,
        region: Optional[str] = None,
        storage_url: Optional[str] = None,
        progress_cb: Optional[Callable[[int], None]] = None,
    ):
        """
        Upload all files from a local directory to the specified S3 bucket, preserving structure.

        Uploads files concurrently with automatic rate limiting if max_concurrency is set.
        Directory structure under the specified local directory is preserved in the S3 bucket.

        Args:
            dir_path: Local directory path containing files to upload
            bucket: Bucket name
            dir_key_prefix: Prefix for objects to upload (e.g., "documents/reports/")
            region: Override default region
            storage_url: Override default storage URL
            progress_cb: Optional progress callback called for each uploaded chunk

        Example:
            # Upload entire directory
            await client.upload_dir(
                dir_path="./data",
                bucket="my-bucket",
                dir_key_prefix="collections/"
            )
            # Uploads: ./data/collection1/img_1.png -> collections/collection1/img_1.png, etc.

            # With progress tracking for each file
            from tqdm import tqdm

            with tqdm(unit='B', unit_scale=True, desc="Uploading") as pbar:
                await client.upload_dir(
                    dir_path="./data",
                    bucket="my-bucket",
                    dir_key_prefix="collections/",
                    progress_cb=pbar.update
                )
        """

        if dir_key_prefix and dir_key_prefix[-1] != "/":
            dir_key_prefix = dir_key_prefix + "/"

        async def _items_iterator():
            for file_path in iter_files(dir_path, recursive=True):
                yield file_path

        async def _worker_function(file_path: str):
            relative_path = Path(file_path).relative_to(dir_path)
            item_key = Path(dir_key_prefix, relative_path).as_posix()
            await self.upload(
                file_path=file_path,
                bucket=bucket,
                key=item_key,
                region=region,
                storage_url=storage_url,
                progress_cb=progress_cb,
            )

        await process_queue(
            _items_iterator(),
            _worker_function,
            concurrency=100,  # 100 simultaneously opened files
        )


class StorageApi:
    """
    Ovalbee API connection to Storage service.
    """

    def __init__(self):
        pass

    # ----------------------------------------------------------------
    # --- Methods for tests ------------------------------------------
    # ----------------------------------------------------------------
    def download_from_minio(self, file_id: str, dst_path: str) -> bytes:

        client = self.get_minio_client()

        if isinstance(file_id, str) and "/" in file_id:
            bucket, src_path = file_id.split("/", 1)
        else:
            raise NotImplementedError()

        self.minio_download_file(client, bucket, src_path, dst_path)

    def upload_to_minio(self, src_path: str, dst_path: str) -> str:

        client = self.get_minio_client()

        if isinstance(dst_path, str) and "/" in dst_path:
            bucket, key = dst_path.split("/", 1)
        else:
            raise NotImplementedError()

        self.minio_upload_file(client, bucket, key, src_path)

    def list_minio_keys(self, prefix: str = "") -> List[str]:

        client = self.get_minio_client()

        if isinstance(prefix, str) and "/" in prefix:
            bucket, pre = prefix.split("/", 1)
        else:
            raise NotImplementedError()

        return self.minio_list_keys(client, bucket, pre)

    # ----------------------------------------------------------------
    # --- Methods for tests ------------------------------------------
    # ----------------------------------------------------------------

    def _parse_endpoint_for_minio(self) -> Tuple[str, bool]:
        raw = os.getenv("MINIO_ROOT_URL", "http://localhost:9000")
        parsed = urlparse(raw if "://" in raw else f"http://{raw}")
        endpoint = parsed.netloc or parsed.path
        secure = parsed.scheme == "https"
        return endpoint, secure

    def _endpoint_url_for_boto3(self) -> str:
        raw = os.getenv("MINIO_ROOT_URL", "http://localhost:9000")
        if "://" not in raw:
            return f"http://{raw}"
        return raw

    def _aws_creds(self) -> Tuple[str, str]:
        return (
            os.getenv("MINIO_ROOT_USER", "minioadmin"),
            os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        )

    def get_minio_client(self) -> Minio:
        endpoint, secure = self._parse_endpoint_for_minio()
        user, password = self._aws_creds()
        return Minio(endpoint, access_key=user, secret_key=password, secure=secure)

    def get_boto3_client(self):
        user, password = self._aws_creds()
        return boto3_client(
            "s3",
            aws_access_key_id=user,
            aws_secret_access_key=password,
            endpoint_url=self._endpoint_url_for_boto3(),
        )

    def get_aioboto3_client(self):
        user, password = self._aws_creds()
        session = Session()
        return session.client(
            "s3",
            aws_access_key_id=user,
            aws_secret_access_key=password,
            endpoint_url=self._endpoint_url_for_boto3(),
        )

    def ensure_bucket_minio(self, client: Minio, bucket: str) -> None:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)

    def ensure_bucket_boto3(self, s3, bucket: str) -> None:
        try:
            s3.head_bucket(Bucket=bucket)
        except ClientError:
            s3.create_bucket(Bucket=bucket)

    async def ensure_bucket_aioboto3(self, bucket: str) -> None:
        async with self.get_aioboto3_client() as s3:
            try:
                await s3.head_bucket(Bucket=bucket)
            except Exception:
                await s3.create_bucket(Bucket=bucket)

    def minio_upload_file(self, client: Minio, bucket: str, key: str, file_path: str) -> None:
        self.ensure_bucket_minio(client, bucket)
        client.fput_object(bucket, key, file_path)

    def minio_download_file(self, client: Minio, bucket: str, key: str, dest_path: str) -> None:
        obj = client.get_object(bucket, key)
        try:
            with open(dest_path, "wb") as f:
                for data in obj.stream(32 * 1024):
                    f.write(data)
        finally:
            obj.close()
            obj.release_conn()

    def minio_list_keys(self, client: Minio, bucket: str, prefix: str = "") -> List[str]:
        return [o.object_name for o in client.list_objects(bucket, prefix=prefix, recursive=True)]

    async def async_minio_upload_file(
        self, client: Minio, bucket: str, key: str, file_path: str
    ) -> None:
        await asyncio.to_thread(self.minio_upload_file, client, bucket, key, file_path)

    async def async_minio_download_file(
        self, client: Minio, bucket: str, key: str, dest_path: str
    ) -> None:
        await asyncio.to_thread(self.minio_download_file, client, bucket, key, dest_path)

    async def async_minio_list_keys(
        self, client: Minio, bucket: str, prefix: str = ""
    ) -> List[str]:
        return await asyncio.to_thread(self.minio_list_keys, client, bucket, prefix)

    def boto3_upload_file(self, s3, bucket: str, key: str, file_path: str) -> None:
        self.ensure_bucket_boto3(s3, bucket)
        s3.upload_file(Filename=file_path, Bucket=bucket, Key=key)

    def boto3_download_file(self, s3, bucket: str, key: str, dest_path: str) -> None:
        s3.download_file(Bucket=bucket, Key=key, Filename=dest_path)

    def boto3_list_keys(self, s3, bucket: str, prefix: str = "") -> List[str]:
        keys: List[str] = []
        continuation_token: Optional[str] = None
        while True:
            kwargs = {"Bucket": bucket, "Prefix": prefix}
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token
            resp = s3.list_objects_v2(**kwargs)
            contents = resp.get("Contents", [])
            keys.extend(obj["Key"] for obj in contents)
            if resp.get("IsTruncated"):
                continuation_token = resp.get("NextContinuationToken")
            else:
                break
        return keys

    async def async_boto3_upload_file(self, s3, bucket: str, key: str, file_path: str) -> None:
        await asyncio.to_thread(self.boto3_upload_file, s3, bucket, key, file_path)

    async def async_boto3_download_file(self, s3, bucket: str, key: str, dest_path: str) -> None:
        await asyncio.to_thread(self.boto3_download_file, s3, bucket, key, dest_path)

    async def async_boto3_list_keys(self, s3, bucket: str, prefix: str = "") -> List[str]:
        return await asyncio.to_thread(self.boto3_list_keys, s3, bucket, prefix)

    async def aioboto3_upload_file(self, bucket: str, key: str, file_path: str) -> None:
        await self.ensure_bucket_aioboto3(bucket)

        data = await asyncio.to_thread(lambda p=file_path: open(p, "rb").read())

        async with self.get_aioboto3_client() as s3:
            await s3.put_object(Bucket=bucket, Key=key, Body=data)

    async def aioboto3_download_file(self, bucket: str, key: str, dest_path: str) -> None:
        async with self.get_aioboto3_client() as s3:
            resp = await s3.get_object(Bucket=bucket, Key=key)
            body = await resp["Body"].read()

        await asyncio.to_thread(lambda p=dest_path, b=body: open(p, "wb").write(b))

    async def aioboto3_list_keys(self, bucket: str, prefix: str = "") -> List[str]:
        keys: List[str] = []
        continuation_token: Optional[str] = None
        async with self.get_aioboto3_client() as s3:
            while True:
                kwargs = {"Bucket": bucket, "Prefix": prefix}
                if continuation_token:
                    kwargs["ContinuationToken"] = continuation_token
                resp = await s3.list_objects_v2(**kwargs)
                contents = resp.get("Contents", [])
                keys.extend(obj["Key"] for obj in contents)
                if resp.get("IsTruncated"):
                    continuation_token = resp.get("NextContinuationToken")
                else:
                    break
        return keys

    async def async_minio_upload_many(
        self,
        client: Minio,
        bucket: str,
        items: Sequence[Tuple[str, str]],
        max_concurrency: int = 10,
    ) -> None:
        sem = asyncio.Semaphore(max_concurrency)

        async def _one(k: str, p: str):
            async with sem:
                await self.async_minio_upload_file(client, bucket, k, p)

        await asyncio.gather(*(_one(k, p) for k, p in items))

    async def async_boto3_upload_many(
        self, s3, bucket: str, items: Sequence[Tuple[str, str]], max_concurrency: int = 10
    ) -> None:
        sem = asyncio.Semaphore(max_concurrency)

        async def _one(k: str, p: str):
            async with sem:
                await self.async_boto3_upload_file(s3, bucket, k, p)

        await asyncio.gather(*(_one(k, p) for k, p in items))

    async def aioboto3_upload_many(
        self, bucket: str, items: Sequence[Tuple[str, str]], max_concurrency: int = 10
    ) -> None:
        sem = asyncio.Semaphore(max_concurrency)

        async def _read_file(path: str) -> bytes:
            return await asyncio.to_thread(lambda p=path: open(p, "rb").read())

        await self.ensure_bucket_aioboto3(bucket)

        async with self.get_aioboto3_client() as s3:

            async def _one(k: str, p: str):
                async with sem:
                    data = await _read_file(p)
                    await s3.put_object(Bucket=bucket, Key=k, Body=data)

            await asyncio.gather(*(_one(k, p) for k, p in items))
