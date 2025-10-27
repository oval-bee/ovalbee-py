"""
Helpers for loading and persisting Ovalbee environment configuration.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from ovalbee.io.credentials import CredentialConfig

OVALBEE_ENV_FILENAME = "ovalbee.env"


def default_env_path() -> Path:
    """
    Placeholder helper for discovering environment files.
    """

    return Path.home() / OVALBEE_ENV_FILENAME


def load_env(path: Optional[Path] = None) -> None:
    """
    Stubbed environment loader; concrete implementation will be added later.
    """

    _ = path or default_env_path()


def is_development() -> bool:
    mode = os.environ.get("ENV", "development")
    if mode == "production":
        return False
    else:
        return True
