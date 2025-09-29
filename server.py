import json
import os
from typing import Optional, Literal
from mcp.server.fastmcp import FastMCP, Context
import asyncpg
import pandas as pd
import json
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from datetime import datetime
from dotenv import load_dotenv
from db import insert_query
import re
import random
import argparse
import matplotlib.pyplot as plt
import matplotlib
import io
from collections import Counter
import numpy as np

# Import logging functionality
from logging_config import (
    main_logger, 
    perf_logger, 
    db_logger,
    DatabasePerformanceLogger,
    log_execution_time, 
    log_async_operation,
    log_sync_operation,
    performance_metrics
)

try:
    from scipy.interpolate import make_interp_spline
    from scipy import stats
    SCIPY_AVAILABLE = True
    main_logger.info("SciPy library loaded successfully")
except ImportError:
    SCIPY_AVAILABLE = False
    main_logger.warning("SciPy library not available - some plotting features will be limited")

from utils import upload_image_to_s3

load_dotenv()

# Set matplotlib to use non-interactive backend
matplotlib.use("Agg")
main_logger.info("Matplotlib backend set to 'Agg' for non-interactive use")

DATABASE_URL = os.getenv("DATABASE_URL")
main_logger.info(f"Database URL configured: {DATABASE_URL[:50]}...")

parser = argparse.ArgumentParser()
parser.add_argument("--port", action="store", type=int, default=8000)
args = parser.parse_args()
port = args.port

main_logger.info(f"MCP server will run on port: {port}")

mcp = FastMCP("SamaajData MCP server", host="0.0.0.0", port=port)

# Initialize database performance logger
db_perf_logger = DatabasePerformanceLogger()

@log_execution_time("get_db_connection", db_logger)
async def get_db_connection():
    """Get database connection with logging"""
    async with log_async_operation("postgres_connection", db_logger):
        db_logger.info(f"Establishing PostgreSQL connection to: {DATABASE_URL[:50]}...")
        conn = await asyncpg.connect(DATABASE_URL)
        db_logger.debug("PostgreSQL connection established successfully")
        return conn

@mcp.tool()
@log_execution_time("get_valid_categories")
async def get_valid_categories(ctx: Context) -> list[str]:
    """
    Returns the list of valid issue/action/event categories that can be used as a filter. 
    If the user query requires filtering for specific category of issue/action/event, 
    use this method to get the list of valid categories to pick from
    """
    async with log_async_operation("get_valid_categories_query", perf_logger):
        conn: asyncpg.Connection = await get_db_connection()
        
        query = 'SELECT * from "tabEvent Category"'
        db_logger.info(f"Executing query: {query}")
        
        try:
            async with db_perf_logger.log_query(query, operation_type="SELECT"):
                rows = await conn.fetch(query)
            
            await insert_query("get_valid_categories", {})
            
            result = [row["name"] for row in rows]
            main_logger.info(f"Retrieved {len(result)} valid categories")
            
            return result
            
        finally:
            await conn.close()
            db_logger.debug("Database connection closed")

@mcp.tool()
@log_execution_time("get_valid_subcategories")
async def get_valid_subcategories(ctx: Context) -> list[str]:
    """
    Returns the list of valid issue/action/event subcategories that can be used as a filter. 
    If the user query requires filtering for specific subcategory of issue/action/event, 
    use this method to get the list of valid subcategories to pick from. Only pick this 
    if the category is also picked and the user query requires further filtering beyond category.
    """
    async with log_async_operation("get_valid_subcategories_query", perf_logger):
        conn: asyncpg.Connection = await get_db_connection()

        query = 'SELECT * from "tabEvent Sub Category"'
        db_logger.info(f"Executing query: {query}")
        
        try:
            async with db_perf_logger.log_query(query, operation_type="SELECT"):
                rows = await conn.fetch(query)

            await insert_query("get_valid_subcategories", {})

            result = [row["name"] for row in rows]
            main_logger.info(f"Retrieved {len(result)} valid subcategories")
            
            return result
            
        finally:
            await conn.close()
            db_logger.debug("Database connection closed")

@mcp.tool()
@log_execution_time("get_valid_event_types")
async def get_valid_event_types(ctx: Context) -> list[str]:
    """
    Returns the list of valid issue/action/event types that can be used as a filter. 
    If the user query requires filtering for specific type of issue/action/event, 
    use this method to get the list of valid types to pick from.
    """
    async with log_async_operation("get_valid_event_types_query", perf_logger):
        conn: asyncpg.Connection = await get_db_connection()

        query = 'SELECT * from "tabEvent Type"'
        db_logger.info(f"Executing query: {query}")
        
        try:
            async with db_perf_logger.log_query(query, operation_type="SELECT"):
                rows = await conn.fetch(query)

            await insert_query("get_valid_event_types", {})

            result = [row["name"] for row in rows]
            main_logger.info(f"Retrieved {len(result)} valid event types")
            
            return result
            
        finally:
            await conn.close()
            db_logger.debug("Database connection closed")

@mcp.tool()
@log_execution_time("get_data_partners_list")
async def get_data_partners_list(ctx: Context) -> dict:
    """
    Returns the list of organizations that have contributed data to SamaajData along with 
    the corresponding category and subcategory of the data they have contributed to be 
    used for filtering the whole dataset.

    Whenever a user query is made, always begin by calling this tool to get the list of 
    partners and their corresponding category and subcategory. This data can then be used 
    to call the appropriate tools to get the required data with the right 
    partner/category/subcategory filters.
    """
    async with log_async_operation("get_data_partners_list_query", perf_logger):
        conn: asyncpg.Connection = await get_db_connection()

        query = """
            SELECT DISTINCT partner, event_category, event_subcategory
            FROM "Events Metadata"
            WHERE partner IS NOT NULL AND partner <> ''
        """
        
        db_logger.info(f"Executing partners query: {query}")
        
        try:
            async with db_perf_logger.log_query(query, operation_type="SELECT"):
                rows = await conn.fetch(query)

            await insert_query("get_data_partners_list", {})

            result = {
                "result": [
                    {
                        "partner": row["partner"],
                        "category": row["event_category"],
                        "subcategory": row["event_subcategory"],
                    }
                    for row in rows
                ]
            }
            
            main_logger.info(f"Retrieved {len(result['result'])} partner entries")
            
            return result
            
        finally:
            await conn.close()
            db_logger.debug("Database connection closed")

@mcp.tool()
@log_execution_time("test_db_connection")
async def test_db_connection(ctx: Context) -> list[str]:
    """Test database connection and return sample data"""
    async with log_async_operation("test_db_connection_query", perf_logger):
        conn: asyncpg.Connection = await get_db_connection()
        
        query = "SELECT title FROM tabEvents LIMIT 10"
        db_logger.info(f"Testing database with query: {query}")
        
        try:
            async with db_perf_logger.log_query(query, operation_type="SELECT"):
                rows = await conn.fetch(query)
            
            result = [row["title"] for row in rows]
            main_logger.info(f"Database test successful, retrieved {len(result)} sample titles")
            
            return result
            
        finally:
            await conn.close()
            db_logger.debug("Database connection closed")

# Continuing from Part 1...

@mcp.tool()
@log_execution_time("get_event_points_for_area_from_samaajdata")
async def get_event_points_for_area_from_samaajdata(
    ctx: Context,
    aggregation_level: Literal["district", "state", "hobli_name", "grama_panchayath"],
    value: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    category: Optional[str] = None,
    subcategory: Optional[str] = None,
    type: Optional[str] = None,
) -> dict:
    """
    Returns all individual event points (latitude, longitude) for a given area and filters from Samaajdata.
    """
    
    main_logger.info(f"Getting event points for {aggregation_level}={value}")
    
    async with log_async_operation("get_event_points_processing", perf_logger):
        await insert_query(
            "get_event_points_for_area_from_samaajdata",
            {
                "aggregation_level": aggregation_level,
                "value": value,
                "start_date": start_date,
                "end_date": end_date,
                "category": category,
                "subcategory": subcategory,
                "type": type,
            },
        )

        # Date processing
        with log_sync_operation("date_processing", main_logger):
            if start_date:
                start = pd.to_datetime(start_date, dayfirst=True)
                main_logger.debug(f"Parsed start date: {start}")
            else:
                start = datetime(2000, 1, 1)
                main_logger.debug("Using default start date: 2000-01-01")

            if end_date:
                end = pd.to_datetime(end_date, dayfirst=True)
                main_logger.debug(f"Parsed end date: {end}")
            else:
                end = datetime.today()
                main_logger.debug(f"Using current date as end date: {end}")

        # Build filters
        filters = [
            f"e.creation >= '{start.date().isoformat()}'",
            f"e.creation <= '{end.date().isoformat()}'",
            f"l.{aggregation_level} = '{value}'",
        ]
        
        filter_count = len(filters)
        
        if category:
            filters.append(f"e.category = '{category}'")
            filter_count += 1
        if subcategory:
            filters.append(f"e.subcategory = '{subcategory}'")
            filter_count += 1
        if type:
            filters.append(f"e.type = '{type}'")
            filter_count += 1

        where_clause = " AND ".join(filters)
        main_logger.info(f"Applied {filter_count} filters to query")

        await ctx.debug(f"where_clause: {where_clause}")

        query = f"""
        SELECT 
            e.latitude::float AS latitude,
            e.longitude::float AS longitude
        FROM "tabEvents" e
        LEFT JOIN "tabLocation" l ON e.location = l.name
        WHERE 
            e.latitude IS NOT NULL
            AND e.longitude IS NOT NULL
            AND e.latitude ~ '^[0-9.+-]+$'
            AND e.longitude ~ '^[0-9.+-]+$'
            AND {where_clause}
        """

        await ctx.debug(f"Query:\n{query}")
        
        conn: asyncpg.Connection = await get_db_connection()
        
        try:
            async with db_perf_logger.log_query(query, {"filters": filter_count}, "SELECT"):
                rows = await conn.fetch(query)

            result_data = [(row["latitude"], row["longitude"]) for row in rows]
            
            main_logger.info(f"Retrieved {len(result_data)} event points for {aggregation_level}={value}")
            
            # Record performance metrics
            performance_metrics.record_metric("event_points_retrieved", len(result_data), "count")
            
            return {
                "latlong": result_data,
                "description": "For the given query, the lat/long of the event points have been returned. You can use this to display a scatter plot on a map.",
            }
            
        finally:
            await conn.close()
            db_logger.debug("Database connection closed")

@mcp.tool()
@log_execution_time("get_data_count_from_samaajdata")
async def get_data_count_from_samaajdata(
    ctx: Context,
    event_categories: list[str],
    event_subcategories: list[str],
    partners: list[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
) -> dict:
    """
    Returns the count of data matching the given filters on SamaajData.
    Supports filtering by start/end date, city, state, event category, event subcategory, and partner.
    """
    
    main_logger.info(f"Getting data count with filters: categories={len(event_categories)}, "
                    f"subcategories={len(event_subcategories)}, partners={len(partners)}")
    
    async with log_async_operation("get_data_count_processing", perf_logger):
        conn: asyncpg.Connection = await get_db_connection()

        where_clauses = []

        if start_date:
            where_clauses.append(f"em.creation >= '{start_date}'")
        if end_date:
            where_clauses.append(f"em.creation <= '{end_date}'")
        if city:
            where_clauses.append(f"em.city = '{city}'")
        if state:
            where_clauses.append(f"em.state = '{state}'")

        if event_categories:
            cat_list = ", ".join([f"'{cat}'" for cat in event_categories])
            where_clauses.append(f"em.event_category IN ({cat_list})")
        if event_subcategories:
            subcat_list = ", ".join([f"'{subcat}'" for subcat in event_subcategories])
            where_clauses.append(f"em.event_subcategory IN ({subcat_list})")
        if partners:
            partner_list = ", ".join([f"'{partner}'" for partner in partners])
            where_clauses.append(f"em.partner IN ({partner_list})")

        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        main_logger.info(f"Applied {len(where_clauses)} filters to count query")

        query = f"""
            SELECT COUNT(DISTINCT em.event_id) as count
            FROM "Events Metadata" em
            WHERE {where_clause}
        """

        await ctx.debug(f"Query: {query}")
        
        try:
            async with db_perf_logger.log_query(query, {"filter_count": len(where_clauses)}, "COUNT"):
                count_row = await conn.fetchrow(query)
            
            count_result = count_row["count"] if count_row else 0
            main_logger.info(f"Data count result: {count_result}")
            
            # Record performance metrics
            performance_metrics.record_metric("data_count_result", count_result, "count")
            
            return {"count": count_result}
            
        finally:
            await conn.close()
            db_logger.debug("Database connection closed")

@mcp.tool()
@log_execution_time("get_data_metadata_on_samaajdata")
async def get_data_metadata_on_samaajdata(
    ctx: Context,
    event_categories: list[str],
    event_subcategories: list[str],
    partners: list[str],
) -> dict:
    """
    Returns the list of fields for all the data matching the given filters on SamaajData.
    """
    
    main_logger.info(f"Getting data metadata for categories={len(event_categories)}, "
                    f"subcategories={len(event_subcategories)}, partners={len(partners)}")
    
    async with log_async_operation("get_data_metadata_processing", perf_logger):
        conn: asyncpg.Connection = await get_db_connection()
        
        where_clauses = []
        if event_categories:
            cat_list = ", ".join([f"'{cat}'" for cat in event_categories])
            where_clauses.append(f"e.event_category IN ({cat_list})")
        if event_subcategories:
            subcat_list = ", ".join([f"'{subcat}'" for subcat in event_subcategories])
            where_clauses.append(f"e.event_subcategory IN ({subcat_list})")
        if partners:
            partner_list = ", ".join([f"'{partner}'" for partner in partners])
            where_clauses.append(f"e.partner IN ({partner_list})")

        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

        # First, get field names and definitions
        fields_query = f"""
            SELECT DISTINCT
                e.field_name, 
                e.field_definition
            FROM "Events Metadata" e
            WHERE {where_clause}
            ORDER BY e.field_name
        """

        await ctx.debug(f"Fields Query: {fields_query}")
        
        try:
            async with db_perf_logger.log_query(fields_query, {"filter_count": len(where_clauses)}, "SELECT"):
                field_rows = await conn.fetch(fields_query)

            main_logger.info(f"Retrieved {len(field_rows)} unique fields")

            # Then, get example values for each field
            rows = []
            for field_row in field_rows:
                field_name = field_row["field_name"]
                
                async with log_async_operation(f"field_examples_{field_name}", main_logger):
                    cat_list = ", ".join([f"'{cat}'" for cat in event_categories])
                    subcat_list = ", ".join([f"'{subcat}'" for subcat in event_subcategories])
                    partner_list = ", ".join([f"'{partner}'" for partner in partners])

                    examples_query = f"""
                        SELECT DISTINCT field_value
                        FROM "Events Metadata"
                        WHERE field_name = $1
                          AND event_category IN ({cat_list})
                          AND event_subcategory IN ({subcat_list})
                          AND partner IN ({partner_list})
                          AND location_id IS NOT NULL
                          AND field_value IS NOT NULL
                        LIMIT 10
                    """

                    await ctx.debug(f"Examples Query for {field_name}: {examples_query}")

                    async with db_perf_logger.log_query(examples_query, {"field_name": field_name}, "SELECT"):
                        example_rows = await conn.fetch(examples_query, field_name)
                    
                    example_values = [row["field_value"] for row in example_rows]
                    
                    main_logger.debug(f"Field {field_name}: {len(example_values)} examples")

                    rows.append(
                        {
                            "field_name": field_row["field_name"],
                            "field_definition": field_row["field_definition"],
                            "example_values": example_values,
                        }
                    )

            result = {
                "fields": [
                    {
                        "name": row["field_name"],
                        "definition": (
                            f"{row['field_definition']} (Examples: {', '.join([str(v) for v in row['example_values'] if v is not None])})"
                            if row["example_values"]
                            else row["field_definition"]
                        ),
                    }
                    for row in rows
                ]
            }
            
            main_logger.info(f"Metadata processing complete: {len(result['fields'])} fields with examples")
            
            return result
            
        finally:
            await conn.close()
            db_logger.debug("Database connection closed")

# Continuing from Part 2... Here are the plotting functions with comprehensive logging

@mcp.tool()
@log_execution_time("create_pie_chart", perf_logger)
async def create_pie_chart(
    ctx: Context,
    data: list[str],
    title: Optional[str] = None,
    colors: Optional[list[str]] = None,
    width: Optional[int] = 10,
    height: Optional[int] = 8,
    show_percentages: Optional[bool] = True,
    start_angle: Optional[int] = 90,
) -> dict:
    """
    Creates a pie chart image and returns the public URL of the image.
    """
    
    main_logger.info(f"Creating pie chart with {len(data) if data else 0} data points")
    
    async with log_async_operation("pie_chart_creation", perf_logger, include_memory=True):
        await insert_query(
            "create_pie_chart",
            {
                "data": data,
                "title": title,
                "colors": colors,
                "width": width,
                "height": height,
                "show_percentages": show_percentages,
                "start_angle": start_angle,
            },
        )

        try:
            # Validate input data
            with log_sync_operation("input_validation", main_logger):
                if not data:
                    raise ValueError("Data cannot be empty")

                if not isinstance(data, list):
                    raise ValueError("Data must be a list of string values")

                # Filter out None and empty string values
                filtered_data = [
                    str(item).strip() for item in data if item is not None and str(item).strip()
                ]

                if not filtered_data:
                    raise ValueError("No valid data found after filtering empty values")
                
                main_logger.info(f"Data validation complete: {len(filtered_data)} valid items from {len(data)} input items")

            # Count occurrences of each unique value
            with log_sync_operation("data_processing", main_logger):
                value_counts = Counter(filtered_data)
                labels = list(value_counts.keys())
                values = list(value_counts.values())
                
                main_logger.info(f"Data processed: {len(labels)} unique categories")

            # Create figure
            with log_sync_operation("matplotlib_figure_creation", perf_logger):
                fig, ax = plt.subplots(figsize=(width, height))
                main_logger.debug(f"Created matplotlib figure: {width}x{height}")

            # Calculate percentages for legend
            with log_sync_operation("percentage_calculation", main_logger):
                total = sum(values)
                percentages = [(value / total * 100) for value in values]

                # Create labels with percentages for legend
                legend_labels = []
                for i, (label, pct) in enumerate(zip(labels, percentages)):
                    if show_percentages:
                        legend_labels.append(f"{label} ({pct:.1f}%)")
                    else:
                        legend_labels.append(label)

            # Create pie chart
            with log_sync_operation("pie_chart_rendering", perf_logger):
                def autopct_func(pct):
                    if pct < 5.0:  # Small slices - don't show percentage on slice
                        return ""
                    return f"{pct:.1f}%"

                autopct = autopct_func if show_percentages else None

                if show_percentages:
                    wedges, texts, autotexts = ax.pie(
                        values,
                        labels=None,
                        autopct=autopct,
                        startangle=start_angle,
                        colors=colors,
                        pctdistance=0.85,
                    )
                else:
                    wedges, texts = ax.pie(
                        values,
                        labels=None,
                        autopct=autopct,
                        startangle=start_angle,
                        colors=colors,
                    )
                    autotexts = None

                main_logger.debug("Pie chart rendering completed")

            # Add legend and styling
            with log_sync_operation("chart_styling", main_logger):
                ax.legend(
                    wedges,
                    legend_labels,
                    title="Categories",
                    loc="center left",
                    bbox_to_anchor=(1, 0, 0.5, 1),
                    fontsize=10,
                )

                if title:
                    ax.set_title(title, fontsize=16, fontweight="bold", pad=20)

                ax.axis("equal")

                if show_percentages and autotexts:
                    for autotext in autotexts:
                        autotext.set_color("white")
                        autotext.set_fontweight("bold")
                        autotext.set_fontsize(9)

                plt.subplots_adjust(left=0.1, right=0.75)
                plt.tight_layout()

            # Save to bytes buffer
            with log_sync_operation("image_buffer_creation", perf_logger):
                img_buffer = io.BytesIO()
                plt.savefig(
                    img_buffer,
                    format="png",
                    dpi=300,
                    bbox_inches="tight",
                    facecolor="white",
                    edgecolor="none",
                )
                
                # Check buffer size
                img_buffer.seek(0, io.SEEK_END)
                buffer_size = img_buffer.tell()
                img_buffer.seek(0)
                
                main_logger.info(f"Image buffer created: {buffer_size / 1024:.2f} KB")

            # Close the figure to free memory
            with log_sync_operation("matplotlib_cleanup", main_logger):
                plt.close(fig)
                main_logger.debug("Matplotlib figure closed and memory freed")

            # Prepare data summary
            with log_sync_operation("data_summary_preparation", main_logger):
                total_count = sum(values)
                data_summary = {
                    "total_categories": len(labels),
                    "total_items": len(filtered_data),
                    "total_count": total_count,
                    "categories": [
                        {
                            "label": label,
                            "count": count,
                            "percentage": (
                                round((count / total_count) * 100, 2) if total_count > 0 else 0
                            ),
                        }
                        for label, count in zip(labels, values)
                    ],
                }

            # Upload to S3
            async with log_async_operation("s3_upload_pie_chart", perf_logger):
                upload_result = upload_image_to_s3(img_buffer)
                
                if upload_result.get("success"):
                    main_logger.info(f"Pie chart uploaded successfully: {upload_result['public_url']}")
                else:
                    main_logger.error(f"Pie chart upload failed: {upload_result.get('error')}")

            # Record performance metrics
            performance_metrics.record_metric("pie_chart_data_points", len(filtered_data), "count")
            performance_metrics.record_metric("pie_chart_categories", len(labels), "count")
            performance_metrics.record_metric("pie_chart_size", buffer_size / 1024, "KB")

            return {
                "public_url": upload_result["public_url"],
                "data_summary": data_summary,
                "title": title or "Pie Chart",
                "instructions": f"The pie chart has been uploaded to S3 at {upload_result['public_url']}. The data_summary provides details about the chart contents including counts and percentages.",
            }

        except Exception as e:
            error_msg = f"Failed to create pie chart: {str(e)}"
            main_logger.error(error_msg)
            await ctx.debug(f"Error creating pie chart: {str(e)}")
            
            # Record error metrics
            performance_metrics.record_metric("pie_chart_errors", 1, "count")
            
            return {
                "error": error_msg,
                "s3_bucket": None,
                "s3_key": None,
                "public_url": None,
                "chart_type": "pie_chart",
            }

@mcp.tool()
@log_execution_time("create_bar_chart", perf_logger)
async def create_bar_chart(
    ctx: Context,
    data: dict,
    title: Optional[str] = None,
    x_label: Optional[str] = None,
    y_label: Optional[str] = None,
    colors: Optional[list[str]] = None,
    width: Optional[int] = 12,
    height: Optional[int] = 8,
    chart_style: Optional[Literal["grouped", "stacked", "horizontal"]] = "grouped",
    show_values: Optional[bool] = True,
    rotation: Optional[int] = 0,
) -> dict:
    """
    Creates a bar chart image and returns the public URL of the image.
    """
    
    main_logger.info(f"Creating {chart_style} bar chart with {len(data) if data else 0} data series")
    
    async with log_async_operation("bar_chart_creation", perf_logger, include_memory=True):
        await insert_query(
            "create_bar_chart",
            {
                "data": data,
                "title": title,
                "x_label": x_label,
                "y_label": y_label,
                "colors": colors,
                "width": width,
                "height": height,
                "chart_style": chart_style,
                "show_values": show_values,
                "rotation": rotation,
            },
        )

        try:
            # Validate input data
            with log_sync_operation("bar_chart_validation", main_logger):
                if not data or not isinstance(data, dict):
                    raise ValueError("Data must be a non-empty dictionary")

            # Process different data formats
            with log_sync_operation("bar_chart_data_processing", main_logger):
                if "raw_data" in data:
                    # Handle raw data - count occurrences
                    if isinstance(data["raw_data"], list) and data["raw_data"]:
                        if isinstance(data["raw_data"][0], dict):
                            # Complex raw data with grouping
                            df = pd.DataFrame(data["raw_data"])
                            if "category" not in df.columns:
                                raise ValueError(
                                    "Raw data with dictionaries must have 'category' column"
                                )

                            if "group" in df.columns and "value" in df.columns:
                                # Grouped data with explicit values
                                pivot_df = df.pivot_table(
                                    values="value",
                                    index="category",
                                    columns="group",
                                    aggfunc="sum",
                                    fill_value=0,
                                )
                                categories = list(pivot_df.index)
                                series_data = {
                                    col: list(pivot_df[col]) for col in pivot_df.columns
                                }
                            else:
                                # Simple counting by category and group
                                if "group" in df.columns:
                                    pivot_df = (
                                        df.groupby(["category", "group"])
                                        .size()
                                        .unstack(fill_value=0)
                                    )
                                    categories = list(pivot_df.index)
                                    series_data = {
                                        col: list(pivot_df[col]) for col in pivot_df.columns
                                    }
                                else:
                                    # Just count by category
                                    counts = df["category"].value_counts().sort_index()
                                    categories = list(counts.index)
                                    series_data = {"Count": list(counts.values)}
                        else:
                            # Simple list - count occurrences
                            value_counts = Counter(data["raw_data"])
                            categories = list(value_counts.keys())
                            series_data = {"Count": list(value_counts.values())}
                else:
                    # Handle structured data
                    if "categories" not in data:
                        raise ValueError("Data must contain 'categories' key or 'raw_data' key")

                    categories = data["categories"]
                    series_data = {}

                    for key, values in data.items():
                        if key != "categories" and isinstance(values, list):
                            if len(values) != len(categories):
                                raise ValueError(
                                    f"Series '{key}' length ({len(values)}) doesn't match categories length ({len(categories)})"
                                )
                            series_data[key] = values

                if not categories or not series_data:
                    raise ValueError("No valid data found for plotting")
                
                main_logger.info(f"Data processed: {len(categories)} categories, {len(series_data)} series")

            # Create figure
            with log_sync_operation("bar_chart_figure_creation", perf_logger):
                fig, ax = plt.subplots(figsize=(width, height))

            # Create bars based on style
            with log_sync_operation(f"bar_chart_rendering_{chart_style}", perf_logger):
                # Set up colors
                if colors is None:
                    colors = plt.cm.Set3(range(len(series_data)))
                elif len(colors) < len(series_data):
                    # Extend colors if not enough provided
                    default_colors = plt.cm.Set3(range(len(series_data)))
                    colors.extend(default_colors[len(colors) :])

                if chart_style == "horizontal":
                    # Horizontal bar chart implementation
                    y_pos = range(len(categories))
                    if len(series_data) == 1:
                        series_name, values = next(iter(series_data.items()))
                        bars = ax.barh(
                            y_pos, values, color=colors[0] if colors is not None else None
                        )
                        if show_values:
                            for i, (bar, value) in enumerate(zip(bars, values)):
                                ax.text(
                                    bar.get_width() + 0.01 * max(values),
                                    bar.get_y() + bar.get_height() / 2,
                                    f"{value}",
                                    ha="left",
                                    va="center",
                                    fontsize=9,
                                )
                    else:
                        # Multiple series horizontal
                        bar_height = 0.8 / len(series_data)
                        for i, (series_name, values) in enumerate(series_data.items()):
                            y_positions = [
                                y + i * bar_height - (len(series_data) - 1) * bar_height / 2
                                for y in y_pos
                            ]
                            bars = ax.barh(
                                y_positions,
                                values,
                                bar_height,
                                label=series_name,
                                color=colors[i] if colors is not None else None,
                            )
                            if show_values:
                                for bar, value in zip(bars, values):
                                    ax.text(
                                        bar.get_width()
                                        + 0.01 * max(max(v) for v in series_data.values()),
                                        bar.get_y() + bar.get_height() / 2,
                                        f"{value}",
                                        ha="left",
                                        va="center",
                                        fontsize=8,
                                    )
                        ax.legend()

                    ax.set_yticks(y_pos)
                    ax.set_yticklabels(categories)
                    if x_label:
                        ax.set_xlabel(x_label)
                    if y_label:
                        ax.set_ylabel(y_label)

                elif chart_style == "stacked":
                    # Stacked bar chart implementation
                    x_pos = range(len(categories))
                    bottoms = [0] * len(categories)

                    for i, (series_name, values) in enumerate(series_data.items()):
                        bars = ax.bar(
                            x_pos,
                            values,
                            bottom=bottoms,
                            label=series_name,
                            color=colors[i] if colors is not None else None,
                        )

                        if show_values:
                            for j, (bar, value, bottom) in enumerate(
                                zip(bars, values, bottoms)
                            ):
                                if value > 0:  # Only show non-zero values
                                    ax.text(
                                        bar.get_x() + bar.get_width() / 2,
                                        bottom + value / 2,
                                        f"{value}",
                                        ha="center",
                                        va="center",
                                        fontsize=8,
                                    )

                        # Update bottoms for next series
                        bottoms = [b + v for b, v in zip(bottoms, values)]

                    ax.legend()
                    ax.set_xticks(x_pos)
                    ax.set_xticklabels(categories, rotation=rotation)
                    if x_label:
                        ax.set_xlabel(x_label)
                    if y_label:
                        ax.set_ylabel(y_label)

                else:  # grouped (default)
                    # Grouped bar chart implementation
                    x_pos = range(len(categories))
                    if len(series_data) == 1:
                        # Single series
                        series_name, values = next(iter(series_data.items()))
                        bars = ax.bar(
                            x_pos, values, color=colors[0] if colors is not None else None
                        )
                        if show_values:
                            for bar, value in zip(bars, values):
                                ax.text(
                                    bar.get_x() + bar.get_width() / 2,
                                    bar.get_height() + 0.01 * max(values),
                                    f"{value}",
                                    ha="center",
                                    va="bottom",
                                    fontsize=9,
                                )
                    else:
                        # Multiple series grouped
                        bar_width = 0.8 / len(series_data)
                        for i, (series_name, values) in enumerate(series_data.items()):
                            x_positions = [
                                x + i * bar_width - (len(series_data) - 1) * bar_width / 2
                                for x in x_pos
                            ]
                            bars = ax.bar(
                                x_positions,
                                values,
                                bar_width,
                                label=series_name,
                                color=colors[i] if colors is not None else None,
                            )
                            if show_values:
                                for bar, value in zip(bars, values):
                                    ax.text(
                                        bar.get_x() + bar.get_width() / 2,
                                        bar.get_height()
                                        + 0.01 * max(max(v) for v in series_data.values()),
                                        f"{value}",
                                        ha="center",
                                        va="bottom",
                                        fontsize=8,
                                    )
                        ax.legend()

                    ax.set_xticks(x_pos)
                    ax.set_xticklabels(categories, rotation=rotation)
                    if x_label:
                        ax.set_xlabel(x_label)
                    if y_label:
                        ax.set_ylabel(y_label)

            # Set title and layout
            with log_sync_operation("bar_chart_styling", main_logger):
                if title:
                    ax.set_title(title, fontsize=16, fontweight="bold", pad=20)
                plt.tight_layout()

            # Save to bytes buffer
            with log_sync_operation("bar_chart_buffer_creation", perf_logger):
                img_buffer = io.BytesIO()
                plt.savefig(
                    img_buffer,
                    format="png",
                    dpi=300,
                    bbox_inches="tight",
                    facecolor="white",
                    edgecolor="none",
                )
                
                # Check buffer size
                img_buffer.seek(0, io.SEEK_END)
                buffer_size = img_buffer.tell()
                img_buffer.seek(0)
                
                main_logger.info(f"Bar chart buffer created: {buffer_size / 1024:.2f} KB")

            # Close the figure to free memory
            with log_sync_operation("bar_chart_cleanup", main_logger):
                plt.close(fig)

            # Prepare data summary
            data_summary = {
                "chart_style": chart_style,
                "categories": categories,
                "series_count": len(series_data),
                "series_names": list(series_data.keys()),
                "total_data_points": sum(len(values) for values in series_data.values()),
            }

            # Upload to S3
            async with log_async_operation("s3_upload_bar_chart", perf_logger):
                upload_result = upload_image_to_s3(img_buffer)

            # Record performance metrics
            performance_metrics.record_metric("bar_chart_categories", len(categories), "count")
            performance_metrics.record_metric("bar_chart_series", len(series_data), "count")
            performance_metrics.record_metric("bar_chart_size", buffer_size / 1024, "KB")

            return {
                "public_url": upload_result["public_url"],
                "data_summary": data_summary,
                "title": title or "Bar Chart",
                "instructions": f"The bar chart has been uploaded to S3 at {upload_result['public_url']}. The data_summary provides details about the chart configuration and data.",
            }

        except Exception as e:
            error_msg = f"Failed to create bar chart: {str(e)}"
            main_logger.error(error_msg)
            await ctx.debug(f"Error creating bar chart: {str(e)}")
            
            # Record error metrics
            performance_metrics.record_metric("bar_chart_errors", 1, "count")
            
            return {
                "error": error_msg,
                "s3_bucket": None,
                "s3_key": None,
                "public_url": None,
            }

# Note: Similar logging patterns should be applied to all other plotting functions
# (create_line_plot, create_histogram, create_box_plot, create_scatter_plot, 
#  create_donut_chart, create_heatmap, create_area_chart)
# Each function should follow the same pattern of:
# 1. Overall timing with log_execution_time decorator
# 2. Main operation timing with log_async_operation
# 3. Individual step timing with log_sync_operation
# 4. Performance metrics recording
# 5. Error logging and metrics

if __name__ == "__main__":
    main_logger.info("Starting MCP server...")
    mcp.run(transport="sse")
    main_logger.info("MCP server started successfully")