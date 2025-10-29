"""
Credential models and helpers used by the authentication layer.
"""

from __future__ import annotations

from typing import Optional
from urllib.parse import urlparse

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


def _is_ssl_url(url: str) -> bool:
    """_is_ssl_url"""
    parsed_url = urlparse(url)
    return parsed_url.scheme == "https"


def _normalize_url(url: Optional[str]) -> str:
    """_normalize_url"""
    if url is None:
        return ""
    parsed_url = urlparse(url)
    if not parsed_url.scheme:
        url = "http://" + url
    return url


class MinioCredentials(BaseSettings):
    """
    Settings model for MinIO credentials via environment variables
    or other settings sources supported by `pydantic-settings`.
    """

    MINIO_ROOT_USER: Optional[SecretStr] = None
    MINIO_ROOT_PASSWORD: Optional[SecretStr] = None
    MINIO_ROOT_URL: Optional[str] = None

    MINIO_REGION: Optional[str] = None
    AWS_REGION: Optional[str] = None
    AWS_DEFAULT_REGION: Optional[str] = None

    model_config = SettingsConfigDict(
        env_file="~/ovalbee.env",
        # frozen=True
        extra="ignore",
    )

    def get_region(self) -> Optional[str]:
        """Get the MinIO/AWS region from the available environment variables."""
        return self.MINIO_REGION or self.AWS_REGION or self.AWS_DEFAULT_REGION or "us-east-1"

    def validate_credentials(self) -> bool:
        """Validate that all required MinIO credentials are present."""
        try:
            assert all(
                [
                    self.MINIO_ROOT_USER is not None,
                    self.MINIO_ROOT_PASSWORD is not None,
                    self.MINIO_ROOT_URL is not None,
                ]
            )
        except AssertionError:
            raise ValueError("Storage credentials are missing.")


class CredentialConfig(BaseSettings):
    """
    Settings model for credential configuration via environment variables
    or other settings sources supported by `pydantic-settings`.
    """

    SERVER_ADDRESS: Optional[str] = None
    API_TOKEN: Optional[SecretStr] = None
    REFRESH_TOKEN: Optional[SecretStr] = None

    # MinIO credentials
    MINIO_ROOT_USER: Optional[SecretStr] = None
    MINIO_ROOT_PASSWORD: Optional[SecretStr] = None
    MINIO_ROOT_URL: Optional[str] = None

    model_config = SettingsConfigDict(
        env_file="ovalbee.env",
        # frozen=True
    )
