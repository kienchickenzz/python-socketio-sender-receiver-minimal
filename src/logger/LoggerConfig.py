"""
LoggerConfig - Configuration cho Logger

Chứa các thông số cấu hình:
- log_dir: Thư mục chứa file log
- backup_days: Số ngày giữ lại log
- project_root: Đường dẫn gốc của project để tính relative path
"""
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class LoggerConfig:
    """
    Configuration cho Logger.

    Attributes:
        project_root: Đường dẫn gốc của project (để tính relative path)
        log_dir: Thư mục chứa file log
        backup_days: Số ngày giữ lại backup log
    """

    project_root: Path
    log_dir: str = "logs"
    backup_days: int = 30
