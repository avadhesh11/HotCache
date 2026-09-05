"""
Serialization utilities for HotCache payloads.
"""

import json
from typing import Any


class CacheSerializer:
    """JSON serializer for cache values."""

    @staticmethod
    def dumps(value: Any) -> str:
        """Serialize Python object to string."""
        return json.dumps(value, default=str)

    @staticmethod
    def loads(raw: str) -> Any:
        """Deserialize string back to Python object."""
        if raw is None:
            return None
        return json.loads(raw)
