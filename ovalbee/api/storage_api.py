import asyncio
import os
from typing import List, Optional, Sequence, Tuple
from urllib.parse import urlparse

from aioboto3 import Session
from boto3 import client as boto3_client
from botocore.exceptions import ClientError
from minio import Minio


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
