"""
LoggerFactory - Factory để tạo Logger

Trách nhiệm:
- Tạo logging.Logger instance với custom formatter
- Singleton pattern để reuse logger
- Hỗ trợ console và file handler với rotation
"""
import logging
import logging.handlers
import os

from src.logger.LoggerConfig import LoggerConfig
from src.logger.RelativePathFormatter import RelativePathFormatter


class LoggerFactory:
    """
    Factory để tạo Logger instance.

    Hỗ trợ singleton để reuse logger across components.

    Example:
        config = LoggerConfig(
            project_root=Path(__file__).parent.parent,
            log_dir="logs",
            backup_days=7
        )
        factory = LoggerFactory(config)
        logger = factory.get_instance()
    """

    _instance: logging.Logger | None = None

    def __init__(self, config: LoggerConfig) -> None:
        """
        Initialize factory với config.

        Args:
            config: Logger configuration
        """
        self._config = config

    def create(self) -> logging.Logger:
        """
        Tạo logging.Logger instance mới.

        Returns:
            logging.Logger: Logger instance với console và file handlers
        """
        logger = logging.getLogger("app")
        logger.setLevel(logging.DEBUG) # Hardcode log level DEBUG

        # Tránh duplicate handlers nếu logger đã tồn tại
        if logger.handlers:
            return logger

        # Tạo formatter với relative path
        formatter = RelativePathFormatter(self._config.project_root)

        # Tạo thư mục log
        os.makedirs(self._config.log_dir, exist_ok=True)

        # File handler với rotation hàng ngày
        file_handler = logging.handlers.TimedRotatingFileHandler(
            filename=os.path.join(self._config.log_dir, "app.log"),
            when="D",
            interval=1,
            backupCount=self._config.backup_days,
            encoding="utf-8",
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(formatter)

        # Thêm handlers vào logger
        logger.addHandler(file_handler) # Ghi log vào file, hữu ích cho debug lâu dài
        logger.addHandler(console_handler) # Ghi log ra console, bỏ qua khi chạy trong môi trường production

        return logger

    def get_instance(self) -> logging.Logger:
        """
        Lấy singleton Logger instance.

        Returns:
            logging.Logger: Singleton logger
        """
        if LoggerFactory._instance is None:
            LoggerFactory._instance = self.create()
        return LoggerFactory._instance

    @classmethod
    def close_instance(cls) -> None:
        """Đóng singleton instance và cleanup handlers."""
        if cls._instance is not None:
            for handler in cls._instance.handlers[:]:
                handler.close()
                cls._instance.removeHandler(handler)
            cls._instance = None
