"""
JsonSerializer - JSON serializer cho Kafka messages

Trách nhiệm:
- Serialize Python dict → bytes
- Deserialize bytes → dict
"""
import json


class JsonSerializer:
    """JSON Serializer đơn giản cho Kafka messages."""

    def __init__(self, encoding: str = "utf-8") -> None:
        """
        Khởi tạo serializer.

        Args:
            encoding (str): Encoding cho bytes
        """
        self._encoding = encoding

    def serialize(self, data: dict) -> bytes:
        """
        Serialize dict → bytes.

        Args:
            data (dict): Data cần serialize

        Returns:
            bytes: JSON bytes
        """
        return json.dumps(data).encode(self._encoding)

    def deserialize_raw(self, data: bytes) -> dict:
        """
        Deserialize bytes → dict.

        Args:
            data (bytes): JSON bytes

        Returns:
            dict: Parsed data
        """
        return json.loads(data.decode(self._encoding))
