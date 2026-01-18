"""
IEventPublisher - Interface cho domain-specific event creation

Trách nhiệm:
- Định nghĩa contract cho create_event() (domain-specific)
- Sử dụng cho DI/testing

Note:
- Chỉ có create_event() abstract method
- publish() sẽ được implement trong BaseEventPublisher (generic, reusable)
- Domain publishers kế thừa CẢ HAI: BaseEventPublisher và IEventPublisher
"""
from abc import ABC, abstractmethod
from typing import Any


class IEventPublisher(ABC):
    """Interface định nghĩa contract cho event creation."""

    @abstractmethod
    def create_event(self, payload: dict[str, Any]):
        """
        Tạo và publish event với domain-specific business logic.

        Args:
            payload (dict[str, Any]): Event payload (domain-specific data)
            key (str | None): Partition key (optional)

        Raises:
            ValueError: Nếu payload không hợp lệ
        """
        pass
