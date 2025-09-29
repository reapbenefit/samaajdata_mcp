# Logging System Usage Guide

## Overview

I've implemented a comprehensive logging system for your SamaajData project that tracks time consumption across all major components. The system provides detailed performance monitoring, error tracking, and metrics collection.

## What's Been Added

### 1. Core Logging Configuration (`logging_config.py`)
- **Performance-focused logging** with millisecond precision
- **Multiple specialized loggers** for different components
- **Automatic time tracking** decorators and context managers
- **Memory monitoring** for resource-intensive operations
- **Performance metrics collection** and reporting

### 2. Database Logging (`db.py`)
- **Connection timing** for database operations
- **Query execution tracking** with slow query detection
- **Transaction monitoring** with rollback logging
- **Error context** for database failures

### 3. S3 Upload Logging (`utils.py`)
- **Upload timing** and buffer size monitoring
- **Large file detection** and warnings
- **Credential method logging** (IAM vs explicit)
- **Error tracking** with detailed context

### 4. MCP Server Logging (`server.py`)
- **Tool execution timing** for all MCP tools
- **Plotting operation breakdown** with step-by-step timing
- **Data processing metrics** (rows processed, file sizes)
- **Memory usage monitoring** for chart generation

### 5. FastAPI Application Logging (`agent.py`)
- **Request/response middleware** with unique request IDs
- **Agent processing timing** from query to response
- **Context building monitoring** for chat history
- **Guardrail execution tracking**

## Log Files Generated

The system creates organized log files in the `logs/` directory:

```
logs/
├── samaajdata.log      # All application logs
├── performance.log     # Performance-specific metrics
└── errors.log          # Error-only logs
```

## Key Features

### 1. Automatic Time Tracking

**Function-level timing:**
```python
@log_execution_time("function_name", logger)
async def my_function():
    # Automatically logs start, end, and duration
    pass
```

**Operation-level timing:**
```python
async with log_async_operation("operation_name", logger):
    # Complex operation here
    # Automatically logs duration and handles errors
```

### 2. Performance Metrics Collection

The system automatically tracks:
- **Database query times** and row counts
- **Image generation times** and file sizes
- **S3 upload times** and buffer sizes
- **API request processing times**
- **Agent execution times**

### 3. Request Tracking

Every HTTP request gets:
- **Unique request ID** for tracing
- **Processing time** measurement
- **Status code** tracking
- **Custom headers** with timing info

### 4. Error Context

Errors include:
- **Detailed context** about what was being processed
- **Timing information** up to the point of failure
- **Input parameters** and sizes
- **Stack traces** for debugging

## How to Monitor Performance

### 1. Real-time Monitoring

**Console Output:** Watch the console for real-time performance information:
```
2024-01-15 10:30:15.123 | PERF | Starting execution: create_pie_chart
2024-01-15 10:30:15.456 | PERF | Operation completed: matplotlib_figure_creation | Duration: 234.56ms
2024-01-15 10:30:16.789 | PERF | Completed execution: create_pie_chart | Duration: 1666.67ms
```

### 2. Log File Analysis

**Performance Log:** Check `logs/performance.log` for timing metrics:
- Function execution times
- Operation breakdowns
- Memory usage patterns
- Request processing times

**Main Log:** Check `logs/samaajdata.log` for detailed operations:
- Database connection details
- Data processing steps
- File size information
- Configuration details

### 3. Metrics Endpoint

Access `/metrics` endpoint to get a performance summary:
```bash
curl http://localhost:8000/metrics
```

This logs a summary of all collected metrics to the performance log.

## Identifying Performance Bottlenecks

### 1. Database Operations
**Look for:**
- Slow query warnings (>1 second)
- Large result set processing
- Connection establishment times
- Transaction rollback frequency

**Example Log:**
```
2024-01-15 10:30:15.123 | samaajdata.database | WARNING | Slow query detected: 1500.00ms - SELECT * FROM "Events Metadata"
```

### 2. Image Generation
**Look for:**
- Chart creation times by type
- Matplotlib memory usage
- Large image file warnings
- Step-by-step timing breakdowns

**Example Log:**
```
2024-01-15 10:30:15.123 | PERF | Operation completed: pie_chart_creation | Duration: 2345.67ms | Memory: 45.67MB
```

### 3. S3 Uploads
**Look for:**
- Upload times vs file sizes
- Large file warnings (>5MB)
- Network-related failures
- Buffer creation times

**Example Log:**
```
2024-01-15 10:30:15.123 | samaajdata | WARNING | Large image detected: 7.89 MB
2024-01-15 10:30:16.789 | PERF | Operation completed: s3_upload_7890123_bytes | Duration: 3456.78ms
```

### 4. Agent Processing
**Look for:**
- Total request processing times
- Context building overhead
- Agent execution time
- Tool usage patterns

**Example Log:**
```
2024-01-15 10:30:15.123 | samaajdata.performance | INFO | Request completed: abc12345 -> /respond | Duration: 5678.90ms
```

## Performance Optimization Tips

### 1. Database Optimization
- **Monitor slow queries** and add indexes
- **Check connection times** for network issues
- **Review large result sets** for pagination needs

### 2. Image Generation Optimization
- **Monitor memory usage** during chart creation
- **Track matplotlib cleanup** to prevent memory leaks
- **Optimize chart complexity** for large datasets

### 3. S3 Upload Optimization
- **Monitor file sizes** and consider compression
- **Check upload times** vs network capacity
- **Review buffer creation** efficiency

### 4. Agent Processing Optimization
- **Monitor context building** overhead
- **Track tool execution** patterns
- **Review query complexity** impact

## Custom Logging

To add logging to new functions:

```python
from logging_config import log_execution_time, log_async_operation, main_logger

@log_execution_time("my_function", main_logger)
async def my_function():
    async with log_async_operation("complex_operation", main_logger):
        # Your code here
        pass
```

## Troubleshooting

### 1. Missing Log Files
- Check that the `logs/` directory is created
- Verify write permissions
- Review any startup errors

### 2. Excessive Logging
- Adjust log levels in `logging_config.py`
- Filter specific loggers if needed
- Use log rotation for large files

### 3. Performance Impact
- The logging system is designed to be lightweight
- Most operations add <1ms overhead
- Disable console logging in production if needed

## Best Practices

1. **Monitor regularly** - Check logs daily for performance trends
2. **Set alerts** - Watch for slow query warnings and large file alerts
3. **Track metrics** - Use the `/metrics` endpoint for regular monitoring
4. **Correlate issues** - Use request IDs to trace problems across components
5. **Archive logs** - Implement log rotation for long-running systems

## Integration Complete

The logging system is now fully integrated into your project. Simply run your application and monitor the console output and log files to identify performance bottlenecks and optimize accordingly.