"""
Registry for optional Ovalbee extras (`ovalbee[video]`, `ovalbee[yolo]`, etc.).

The module will eventually expose lightweight discovery hooks so that optional
dependencies can extend SDK functionality without bloating the default install.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable


@dataclass(frozen=True)
class Extra:
    """
    Metadata describing an optional feature bundle.
    """

    name: str
    description: str
    packages: Iterable[str]


EXTRAS: Dict[str, Extra] = {}

def register_extra(extra: Extra) -> None:
    """
    Register an extra feature bundle.

    Args:
        extra: Extra metadata to register.
    """
    EXTRAS[extra.name] = extra

def validate_extra_installed(extra_name: str) -> None:
    """
    Validate that the given extra is installed.

    Args:
        extra_name: Name of the extra to validate.
    Raises:
        ImportError: If the extra is not installed.
    """
    if extra_name not in EXTRAS:
        raise ImportError(f"Ovalbee extra '{extra_name}' is not recognized.")

    extra = EXTRAS[extra_name]
    missing_packages = []
    for package in extra.packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)

    if missing_packages:
        raise ImportError(
            f"The extra '{extra_name}' requires the following packages to be installed: "
            f"{', '.join(missing_packages)}. Please install them to use this feature."
        )
