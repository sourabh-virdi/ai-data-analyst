"""
Logging utilities for AI Data Analyst

Provides structured logging with proper formatting and file rotation.
"""

import logging
import logging.handlers
import os
from pathlib import Path
from typing import Optional
import structlog


def setup_logger(name: str, level: str = "INFO", log_file: Optional[str] = None) -> structlog.BoundLogger:
    """
    Set up structured logger with console and file output
    
    Args:
        name: Logger name
        level: Logging level
        log_file: Optional log file path
    
    Returns:
        Configured structlog logger
    """
    
    # Create logs directory if it doesn't exist
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Configure standard library logging
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=_get_handlers(log_file, level)
    )
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="ISO"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    return structlog.get_logger(name)


def _get_handlers(log_file: Optional[str], level: str) -> list:
    """Get logging handlers for console and file output"""
    
    handlers = []
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, level.upper()))
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    handlers.append(console_handler)
    
    # File handler with rotation
    if log_file:
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(getattr(logging, level.upper()))
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        handlers.append(file_handler)
    
    return handlers


class AnalyticsLogger:
    """Specialized logger for analytics operations"""
    
    def __init__(self, name: str = "analytics"):
        self.logger = setup_logger(name)
    
    def log_query(self, query: str, duration: float, rows_returned: int):
        """Log database query execution"""
        self.logger.info(
            "Database query executed",
            query=query[:100] + "..." if len(query) > 100 else query,
            duration_seconds=duration,
            rows_returned=rows_returned
        )
    
    def log_analysis(self, analysis_type: str, data_source: str, duration: float, result_summary: str):
        """Log data analysis operation"""
        self.logger.info(
            "Data analysis completed",
            analysis_type=analysis_type,
            data_source=data_source,
            duration_seconds=duration,
            result_summary=result_summary
        )
    
    def log_visualization(self, chart_type: str, data_source: str, export_path: str):
        """Log visualization creation"""
        self.logger.info(
            "Visualization created",
            chart_type=chart_type,
            data_source=data_source,
            export_path=export_path
        )
    
    def log_error(self, operation: str, error: Exception, context: dict = None):
        """Log errors with context"""
        self.logger.error(
            "Operation failed",
            operation=operation,
            error=str(error),
            error_type=type(error).__name__,
            context=context or {}
        )
    
    def log_performance(self, operation: str, duration: float, memory_usage: float = None):
        """Log performance metrics"""
        self.logger.info(
            "Performance metrics",
            operation=operation,
            duration_seconds=duration,
            memory_usage_mb=memory_usage
        ) 