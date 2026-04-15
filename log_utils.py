from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from typing import Any

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")


def _normalize_level(level: str | int | None) -> int:
    if isinstance(level, int):
        return level
    if level is None:
        return logging.INFO
    return getattr(logging, str(level).upper(), logging.INFO)


def _sanitize_context_value(value: Any) -> str:
    text = str(value).replace("\n", " ").strip()
    if not text:
        return "-"
    if " " in text:
        return f'"{text}"'
    return text


def _format_context(context: Any) -> str:
    if not context:
        return ""

    if isinstance(context, dict):
        parts: list[str] = []
        for key in sorted(context.keys()):
            val = context.get(key)
            if val is None:
                continue
            parts.append(f"{key}={_sanitize_context_value(val)}")
        return " ".join(parts)

    if isinstance(context, (list, tuple, set)):
        return " ".join(_sanitize_context_value(item) for item in context)

    return _sanitize_context_value(context)


def _process_from_logger_name(logger_name: str) -> str:
    if not logger_name:
        return "app"
    if "." in logger_name:
        return logger_name.split(".")[-1]
    return logger_name


class DBMonitorLogFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    def format(self, record):
        timestamp = self.formatTime(record)
        process_name = str(getattr(record, "process_name", "") or _process_from_logger_name(record.name))
        correlation_id = str(getattr(record, "correlation_id", "-") or "-")
        event_code = str(getattr(record, "event_code", "GENERAL") or "GENERAL")
        message = record.getMessage()
        context_text = _format_context(getattr(record, "context", None))
        source = f"({os.path.basename(record.pathname)}:{record.lineno})"

        if context_text:
            return (
                f"[{timestamp}] [{process_name}] [{record.levelname}] "
                f"[{correlation_id}] [{event_code}] {message} "
                f"[context: {context_text}] {source}"
            )

        return (
            f"[{timestamp}] [{process_name}] [{record.levelname}] "
            f"[{correlation_id}] [{event_code}] {message} {source}"
        )


def setup_process_logger(process_name: str, level: str | int | None = None) -> logging.Logger:
    logger_name = f"dbmonitor.{process_name}"
    logger = logging.getLogger(logger_name)

    if getattr(logger, "_dbmonitor_configured", False):
        return logger

    logger.setLevel(_normalize_level(level or os.getenv("LOG_LEVEL", "INFO")))
    logger.propagate = False

    os.makedirs(LOG_DIR, exist_ok=True)

    max_bytes = int(os.getenv("LOG_MAX_BYTES", str(5 * 1024 * 1024)))
    backup_count = int(os.getenv("LOG_BACKUP_COUNT", "7"))

    formatter = DBMonitorLogFormatter()

    file_handler = RotatingFileHandler(
        os.path.join(LOG_DIR, f"{process_name}.log"),
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    logger._dbmonitor_configured = True
    return logger


def make_correlation_id(prefix: str = "evt") -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    token = uuid.uuid4().hex[:6]
    return f"{prefix}_{stamp}_{token}"


def emit_log(
    logger: logging.Logger,
    level: str | int,
    event_code: str,
    message: str,
    correlation_id: str | None = None,
    context: Any | None = None,
    exc_info: bool = False,
):
    process_name = _process_from_logger_name(logger.name)
    logger.log(
        _normalize_level(level),
        message,
        extra={
            "process_name": process_name,
            "correlation_id": correlation_id or "-",
            "event_code": event_code or "GENERAL",
            "context": context,
        },
        exc_info=exc_info,
    )
