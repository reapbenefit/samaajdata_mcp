import logging
import time
import functools
import asyncio
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Dict
import json
from datetime import datetime
import os

# Configure logging format and levels
class PerformanceFormatter(logging.Formatter):
    """Custom formatter that includes performance metrics"""
    
    def format(self, record):
        # Add timestamp with milliseconds
        record.timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        
        # Build duration and memory strings only if they exist
        duration_str = ""
        memory_str = ""
        
        if hasattr(record, 'duration') and isinstance(record.duration, (int, float)):
            duration_str = f" | Duration: {record.duration * 1000:.2f}ms"
            
        if hasattr(record, 'memory_mb'):
            # Check if it's already a string (from previous formatting) or a number
            if isinstance(record.memory_mb, str):
                memory_str = record.memory_mb if record.memory_mb else ""
            elif isinstance(record.memory_mb, (int, float)):
                memory_str = f" | Memory: {record.memory_mb:.2f}MB"
        
        # Set formatted strings for compatibility with format string
        record.duration_ms = duration_str
        record.memory_mb = memory_str
            
        return super().format(record)

def setup_logging():
    """Setup logging configuration for the entire application"""
    
    # Create logs directory if it doesn't exist
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Main application logger
    logger = logging.getLogger("samaajdata")
    logger.setLevel(logging.DEBUG)
    
    # Performance logger for timing metrics
    perf_logger = logging.getLogger("samaajdata.performance")
    perf_logger.setLevel(logging.INFO)
    
    # Database logger
    db_logger = logging.getLogger("samaajdata.database")
    db_logger.setLevel(logging.INFO)
    
    # Clear existing handlers
    logger.handlers.clear()
    perf_logger.handlers.clear()
    db_logger.handlers.clear()
    
    # Console handler with custom formatter
    console_handler = logging.StreamHandler()
    console_formatter = PerformanceFormatter(
        '%(timestamp)s | %(name)s | %(levelname)s | %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.INFO)
    
    # File handler for all logs
    file_handler = logging.FileHandler(f"{log_dir}/samaajdata.log")
    file_formatter = PerformanceFormatter(
        '%(timestamp)s | %(name)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.DEBUG)
    
    # Performance file handler
    perf_file_handler = logging.FileHandler(f"{log_dir}/performance.log")
    perf_formatter = PerformanceFormatter(
        '%(timestamp)s | PERF | %(message)s%(duration_ms)s'
    )
    perf_file_handler.setFormatter(perf_formatter)
    
    # Error file handler
    error_file_handler = logging.FileHandler(f"{log_dir}/errors.log")
    error_file_handler.setLevel(logging.ERROR)
    error_file_handler.setFormatter(file_formatter)
    
    # Add handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.addHandler(error_file_handler)
    
    perf_logger.addHandler(console_handler)
    perf_logger.addHandler(perf_file_handler)
    
    db_logger.addHandler(console_handler)
    db_logger.addHandler(file_handler)
    
    return logger, perf_logger, db_logger

# Initialize loggers
main_logger, perf_logger, db_logger = setup_logging()

def log_execution_time(func_name: str = None, logger: logging.Logger = None):
    """Decorator to log execution time of functions"""
    def decorator(func):
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            function_name = func_name or f"{func.__module__}.{func.__name__}"
            log = logger or perf_logger
            
            try:
                log.info(f"Starting execution: {function_name}")
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                
                # Create log record with duration
                record = logging.LogRecord(
                    name=log.name,
                    level=logging.INFO,
                    pathname="",
                    lineno=0,
                    msg=f"Completed execution: {function_name}",
                    args=(),
                    exc_info=None
                )
                record.duration = duration
                log.handle(record)
                
                return result
                
            except Exception as e:
                duration = time.time() - start_time
                log.error(f"Failed execution: {function_name} after {duration*1000:.2f}ms - {str(e)}")
                raise
        
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            function_name = func_name or f"{func.__module__}.{func.__name__}"
            log = logger or perf_logger
            
            try:
                log.info(f"Starting async execution: {function_name}")
                result = await func(*args, **kwargs)
                duration = time.time() - start_time
                
                # Create log record with duration
                record = logging.LogRecord(
                    name=log.name,
                    level=logging.INFO,
                    pathname="",
                    lineno=0,
                    msg=f"Completed async execution: {function_name}",
                    args=(),
                    exc_info=None
                )
                record.duration = duration
                log.handle(record)
                
                return result
                
            except Exception as e:
                duration = time.time() - start_time
                log.error(f"Failed async execution: {function_name} after {duration*1000:.2f}ms - {str(e)}")
                raise
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator

@asynccontextmanager
async def log_async_operation(operation_name: str, logger: logging.Logger = None, 
                             include_memory: bool = False):
    """Async context manager for logging operation duration"""
    start_time = time.time()
    log = logger or perf_logger
    
    try:
        log.info(f"Starting operation: {operation_name}")
        yield
        
    except Exception as e:
        duration = time.time() - start_time
        log.error(f"Operation failed: {operation_name} after {duration*1000:.2f}ms - {str(e)}")
        raise
        
    finally:
        duration = time.time() - start_time
        
        # Create log record with duration
        record = logging.LogRecord(
            name=log.name,
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=f"Operation completed: {operation_name}",
            args=(),
            exc_info=None
        )
        record.duration = duration
        
        if include_memory:
            try:
                import psutil
                process = psutil.Process()
                memory_mb = process.memory_info().rss / 1024 / 1024
                record.memory_mb = memory_mb
            except ImportError:
                pass
        
        log.handle(record)

@contextmanager
def log_sync_operation(operation_name: str, logger: logging.Logger = None):
    """Synchronous context manager for logging operation duration"""
    start_time = time.time()
    log = logger or perf_logger
    
    try:
        log.info(f"Starting operation: {operation_name}")
        yield
        
    except Exception as e:
        duration = time.time() - start_time
        log.error(f"Operation failed: {operation_name} after {duration*1000:.2f}ms - {str(e)}")
        raise
        
    finally:
        duration = time.time() - start_time
        
        # Create log record with duration
        record = logging.LogRecord(
            name=log.name,
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=f"Operation completed: {operation_name}",
            args=(),
            exc_info=None
        )
        record.duration = duration
        log.handle(record)

class DatabasePerformanceLogger:
    """Specialized logger for database operations"""
    
    def __init__(self):
        self.logger = db_logger

    @asynccontextmanager
    async def log_query(self, query: str, params: Dict[str, Any] = None, 
                       operation_type: str = "query"):
        """Log database query with timing"""
        start_time = time.time()

        # Sanitize query for logging (avoid dumping entire huge SQL)
        sanitized_query = query[:200] + "..." if len(query) > 200 else query

        try:
            self.logger.info(f"Executing {operation_type}: {sanitized_query}")
            if params:
                self.logger.debug(f"Query parameters: {json.dumps(params, default=str)}")

            yield  # allow async with block to run the query

        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"Query failed after {duration*1000:.2f}ms: {str(e)}")
            self.logger.error(f"Failed query: {sanitized_query}")
            raise

        finally:
            duration = time.time() - start_time
            if duration > 1.0:  # Log slow queries (>1s)
                self.logger.warning(f"Slow query detected: {duration*1000:.2f}ms - {sanitized_query}")
            else:
                self.logger.info(f"Query completed in {duration*1000:.2f}ms")

# Performance metrics collector
class PerformanceMetrics:
    """Collect and log performance metrics"""
    
    def __init__(self):
        self.metrics = {}
        self.logger = perf_logger
    
    def record_metric(self, metric_name: str, value: float, unit: str = "ms"):
        """Record a performance metric"""
        if metric_name not in self.metrics:
            self.metrics[metric_name] = []
        
        self.metrics[metric_name].append(value)
        self.logger.info(f"Metric recorded: {metric_name} = {value}{unit}")
    
    def log_summary(self):
        """Log summary of all metrics"""
        if not self.metrics:
            return
        
        summary = {}
        for metric_name, values in self.metrics.items():
            summary[metric_name] = {
                "count": len(values),
                "avg": sum(values) / len(values),
                "min": min(values),
                "max": max(values)
            }
        
        self.logger.info(f"Performance Summary: {json.dumps(summary, indent=2)}")

# Global performance metrics instance
performance_metrics = PerformanceMetrics()

# Request tracking for FastAPI
class RequestTracker:
    """Track request processing times"""
    
    def __init__(self):
        self.logger = perf_logger
        self.active_requests = {}
    
    def start_request(self, request_id: str, endpoint: str):
        """Start tracking a request"""
        self.active_requests[request_id] = {
            "start_time": time.time(),
            "endpoint": endpoint
        }
        self.logger.info(f"Request started: {request_id} -> {endpoint}")
    
    def end_request(self, request_id: str, status_code: int = 200):
        """End tracking a request"""
        if request_id not in self.active_requests:
            return
        
        request_data = self.active_requests.pop(request_id)
        duration = time.time() - request_data["start_time"]
        
        # Create log record with duration
        record = logging.LogRecord(
            name=self.logger.name,
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=f"Request completed: {request_id} -> {request_data['endpoint']} (Status: {status_code})",
            args=(),
            exc_info=None
        )
        record.duration = duration
        self.logger.handle(record)
        
        # Record metric
        performance_metrics.record_metric(f"request_{request_data['endpoint']}", duration * 1000)

# Global request tracker instance
request_tracker = RequestTracker()