import os
from contextlib import asynccontextmanager
from os.path import exists
import sqlite3
import aiosqlite
import json
from config import sqlite_db_path
from logging_config import (
    db_logger, 
    DatabasePerformanceLogger, 
    log_execution_time, 
    log_async_operation
)

# Initialize database performance logger
db_perf_logger = DatabasePerformanceLogger()

@asynccontextmanager
async def get_new_db_connection():
    conn = None
    try:
        async with log_async_operation("database_connection", db_logger):
            db_logger.info(f"Establishing database connection to {sqlite_db_path}")
            conn = await aiosqlite.connect(sqlite_db_path)
            await conn.execute("PRAGMA synchronous=NORMAL;")
            db_logger.debug("Database connection established successfully")
            yield conn
    except Exception as e:
        db_logger.error(f"Database connection error: {str(e)}")
        if conn:
            try:
                await conn.rollback()  # Rollback on any exception
                db_logger.info("Database transaction rolled back")
            except Exception as rollback_error:
                db_logger.error(f"Rollback failed: {str(rollback_error)}")
        raise  # Re-raise the exception to propagate the error
    finally:
        if conn:
            try:
                await conn.close()
                db_logger.debug("Database connection closed")
            except Exception as close_error:
                db_logger.error(f"Error closing database connection: {str(close_error)}")

@log_execution_time("set_db_defaults", db_logger)
def set_db_defaults():
    db_logger.info(f"Setting database defaults for {sqlite_db_path}")
    conn = sqlite3.connect(sqlite_db_path)
    
    try:
        current_mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
        db_logger.debug(f"Current journal mode: {current_mode}")

        if current_mode.lower() != "wal":
            db_logger.info("Setting journal mode to WAL")
            settings = "PRAGMA journal_mode = WAL;"
            conn.executescript(settings)
            db_logger.info("Database defaults set successfully")
        else:
            db_logger.info("Database defaults already set")
    except Exception as e:
        db_logger.error(f"Error setting database defaults: {str(e)}")
        raise
    finally:
        conn.close()

@log_execution_time("create_tables", db_logger)
async def create_tables(cursor):
    db_logger.info("Creating database tables if they don't exist")
    
    create_query = """
        CREATE TABLE IF NOT EXISTS queries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_type TEXT NOT NULL,
            query_params TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    
    try:
        async with db_perf_logger.log_query(create_query, operation_type="CREATE_TABLE"):
            await cursor.execute(create_query)
        db_logger.info("Tables created successfully")
    except Exception as e:
        db_logger.error(f"Error creating tables: {str(e)}")
        raise

@log_execution_time("init_db", db_logger)
async def init_db():
    db_logger.info("Initializing database")
    
    # Ensure the database folder exists
    db_folder = os.path.dirname(sqlite_db_path)
    if not os.path.exists(db_folder):
        db_logger.info(f"Creating database directory: {db_folder}")
        os.makedirs(db_folder)

    if not exists(sqlite_db_path):
        db_logger.info("Database file doesn't exist, setting defaults for the first time")
        # only set the defaults the first time
        set_db_defaults()
    else:
        db_logger.info("Database file already exists")

    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()

        try:
            # Check if any table is missing and create tables if needed
            await create_tables(cursor)
            await conn.commit()
            db_logger.info("Database initialization completed successfully")

        except Exception as exception:
            db_logger.error(f"Database initialization failed: {str(exception)}")
            # delete db
            if exists(sqlite_db_path):
                os.remove(sqlite_db_path)
                db_logger.warning("Corrupted database file deleted")
            raise exception

@log_execution_time("insert_query", db_logger)
async def insert_query(query_type, query_params):
    db_logger.info(f"Inserting query record: type={query_type}")
    
    insert_sql = "INSERT INTO queries (query_type, query_params) VALUES (?, ?)"
    params_json = json.dumps(query_params)
    
    # Log parameter size for performance monitoring
    param_size = len(params_json)
    if param_size > 10000:  # Log if parameters are large (>10KB)
        db_logger.warning(f"Large query parameters detected: {param_size} bytes")
    
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()
        try:
            async with db_perf_logger.log_query(
                insert_sql, 
                {"query_type": query_type, "param_size": param_size}, 
                "INSERT"
            ):
                await cursor.execute(insert_sql, (query_type, params_json))
                await conn.commit()
            
            db_logger.debug(f"Query record inserted successfully for type: {query_type}")
            
        except Exception as e:
            db_logger.error(f"Error inserting query record: {str(e)}")
            db_logger.error(f"Query type: {query_type}, Params size: {param_size}")
            raise