"""
RelativePathFormatter - Custom Formatter hiển thị relative path

Thay vì hiển thị absolute path hoặc chỉ filename,
formatter này sẽ hiển thị path tương đối từ project root.

Ví dụ:
    Absolute: /home/user/project/src/services/user.py
    Relative: src/services/user.py
"""
import logging
from pathlib import Path


class RelativePathFormatter(logging.Formatter):
    """
    Custom Formatter hiển thị relative path từ project root.

    Format output:
        2026-01-20 09:43:01 [DEBUG   ] src/main.py:25 - Debug message
    """

    def __init__(self, project_root: Path) -> None:
        """
        Initialize formatter với project root.

        Args:
            project_root: Đường dẫn gốc của project
        """
        super().__init__(
            fmt="%(asctime)s [%(levelname)-8s] %(relative_path)s:%(lineno)d - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self._project_root = project_root.resolve()

    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record với relative path.

        Args:
            record: Log record cần format

        Returns:
            Formatted log string
        """
        relative_path: Path | str

        # Tính relative path từ project root
        try:
            absolute_path = Path(record.pathname).resolve()
            relative_path = absolute_path.relative_to(self._project_root)
        except ValueError:
            # Nếu không thể tính relative path, dùng filename
            relative_path = record.filename

        # Thêm relative_path vào record
        record.relative_path = str(relative_path)

        return super().format(record)
