"""
Registry for transform implementations (rotate, resize, format conversion, etc.).
"""

from __future__ import annotations

from typing import Dict, Type

# Concrete registration logic will live here later; the dictionary simply
# reserves the interface.
TRANSFORMS: Dict[str, Type[object]] = {}
