"""
Credential models and helpers used by the authentication layer.
"""

from __future__ import annotations

from typing import Optional

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class CredentialSettings(BaseSettings):
    """
    Settings model for credential configuration via environment variables
    or other settings sources supported by `pydantic-settings`.
    """

    server_address: Optional[str] = None
    api_token: Optional[SecretStr] = None
    refresh_token: Optional[SecretStr] = None

    model_config = SettingsConfigDict(
        env_file="ovalbee.env",
        # frozen=True
    )
