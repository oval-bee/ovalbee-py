"""
Abstract pipeline orchestration for long-running operations.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Operation:
    """
    Placeholder operation descriptor.
    """

    name: str
    parameters: dict
    depends_on: Optional[List[str]] = None
