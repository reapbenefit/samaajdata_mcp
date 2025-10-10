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

# FIXED: Handle missing DATABASE_URL gracefully
if DATABASE_URL:
    main_logger.info(f"Database URL configured: {DATABASE_URL[:50]}...")
else:
    main_logger.warning("DATABASE_URL not set - using environment configuration")

parser = argparse.ArgumentParser()
parser.add_argument("--port", action="store", type=int, default=8000)
args = parser.parse_args()
port = args.port

main_logger.info(f"MCP server will run on port: {port}")

mcp = FastMCP("SamaajData MCP server", host="0.0.0.0", port=port)

# Initialize database performance logger
db_perf_logger = DatabasePerformanceLogger()

# ==================== CITY NAME NORMALIZATION (NEW) ====================

CITY_NAME_MAPPINGS = {
    "bangalore": ["Bangalore", "Bengaluru", "bangalore", "bengaluru"],
    "bengaluru": ["Bangalore", "Bengaluru", "bangalore", "bengaluru"],
    "mumbai": ["Mumbai", "Bombay", "mumbai", "bombay"],
    "bombay": ["Mumbai", "Bombay", "mumbai", "bombay"],
    "delhi": ["Delhi", "New Delhi", "delhi", "new delhi"],
    "kolkata": ["Kolkata", "Calcutta", "kolkata", "calcutta"],
    "calcutta": ["Kolkata", "Calcutta", "kolkata", "calcutta"],
    "chennai": ["Chennai", "Madras", "chennai", "madras"],
    "madras": ["Chennai", "Madras", "chennai", "madras"],
    "pune": ["Pune", "Poona", "pune", "poona"],
    "thiruvananthapuram": ["Thiruvananthapuram", "Trivandrum", "thiruvananthapuram", "trivandrum"],
    "kochi": ["Kochi", "Cochin", "kochi", "cochin"],
    "visakhapatnam": ["Visakhapatnam", "Vizag", "visakhapatnam", "vizag"],
    "vadodara": ["Vadodara", "Baroda", "vadodara", "baroda"]
}

@log_execution_time("normalize_city_name", main_logger)
def normalize_city_name(city: str) -> list[str]:
    """Returns all variations of a city name for robust database queries"""
    city_lower = city.lower().strip()
    
    if city_lower in CITY_NAME_MAPPINGS:
        variations = CITY_NAME_MAPPINGS[city_lower]
        main_logger.debug(f"Found {len(variations)} variations for '{city}': {variations}")
        return variations
    
    variations = [city, city.title(), city.lower(), city.upper()]
    main_logger.debug(f"No predefined mapping for '{city}', using case variations")
    return list(set(variations))

# ==================== URL FORMATTING (NEW) ====================

@log_execution_time("format_urls_as_markdown_links", main_logger)
def format_urls_as_markdown_links(text: str) -> str:
    """Convert plain URLs to markdown links with descriptive titles"""
    if not text or not isinstance(text, str):
        return text
    
    url_pattern = r'(https?://[^\s<>"{}|\\^`\[\]]+)'
    
    def replace_url(match):
        url = match.group(1)
        
        if 'gramvaani.org' in url or 'voice' in url or '.mp3' in url:
            title = "🎤 Listen to Citizen Voice Recording"
        elif 'cloudfront' in url or '.png' in url or '.jpg' in url or '.jpeg' in url:
            title = "📊 View Visualization"
        elif 'tinyurl' in url or 'rb.gy' in url or 'bit.ly' in url:
            title = "🔗 View Data Source"
        elif 'youtube' in url or 'youtu.be' in url:
            title = "▶️ Watch Video"
        elif 'docs.google' in url or 'drive.google' in url:
            title = "📄 View Document"
        elif 'maps.google' in url or 'goo.gl/maps' in url:
            title = "🗺️ View on Map"
        elif '.pdf' in url:
            title = "📑 View PDF Document"
        elif '.csv' in url or '.xlsx' in url:
            title = "📊 Download Data File"
        else:
            title = "🔗 View Resource"
        
        return f"[{title}]({url})"
    
    formatted_text = re.sub(url_pattern, replace_url, text)
    url_count = len(re.findall(url_pattern, text))
    if url_count > 0:
        main_logger.info(f"Formatted {url_count} URLs to markdown links")
    
    return formatted_text

# ==================== DATABASE CONNECTION ====================

@log_execution_time("get_db_connection", db_logger)
async def get_db_connection():
    """Get database connection with logging"""
    async with log_async_operation("postgres_connection", db_logger):
        db_logger.info(f"Establishing PostgreSQL connection to: {DATABASE_URL[:50] if DATABASE_URL else 'Not configured'}...")
        conn = await asyncpg.connect(DATABASE_URL)
        db_logger.debug("PostgreSQL connection established successfully")
        return conn

# ==================== EXISTING TOOLS ====================

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

# ==================== LOCATION DISCOVERY TOOLS (NEW) ====================

@mcp.tool()
@log_execution_time("get_available_locations_for_category")
async def get_available_locations_for_category(
    ctx: Context,
    category: Optional[str] = None,
    subcategory: Optional[str] = None,
    partner: Optional[str] = None,
    location_level: Literal["city", "state", "district"] = "city"
) -> dict:
    """
    Returns the list of locations (cities, states, or districts) that have data available
    for a given category, subcategory, or partner. 
    
    ALWAYS call this tool FIRST when a user asks about data for a specific location
    to verify that location has data and to suggest alternatives if it doesn't.
    """
    
    main_logger.info(f"Getting available {location_level}s for category={category}, "
                    f"subcategory={subcategory}, partner={partner}")
    
    async with log_async_operation("get_available_locations_query", perf_logger):
        conn: asyncpg.Connection = await get_db_connection()
        
        where_clauses = []
        if category:
            where_clauses.append(f"em.event_category = '{category}'")
        if subcategory:
            where_clauses.append(f"em.event_subcategory = '{subcategory}'")
        if partner:
            where_clauses.append(f"em.partner = '{partner}'")
        
        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        query = f"""
            SELECT 
                em.{location_level} as location,
                COUNT(DISTINCT em.event_id) as count
            FROM "Events Metadata" em
            WHERE {where_clause}
                AND em.{location_level} IS NOT NULL
                AND em.{location_level} <> ''
            GROUP BY em.{location_level}
            ORDER BY count DESC
        """
        
        db_logger.info(f"Executing query: {query}")
        
        try:
            async with db_perf_logger.log_query(query, operation_type="SELECT"):
                rows = await conn.fetch(query)
            
            await insert_query("get_available_locations_for_category", {
                "category": category,
                "subcategory": subcategory,
                "partner": partner,
                "location_level": location_level
            })
            
            result = {
                "location_level": location_level,
                "total_locations": len(rows),
                "locations": [
                    {
                        "name": row["location"],
                        "data_count": row["count"]
                    }
                    for row in rows
                ],
                "message": f"Found {len(rows)} {location_level}(s) with data. "
                          f"Use these location names in your queries for accurate results."
            }
            
            if len(rows) == 0:
                result["message"] = (f"No {location_level}s found with data for the given filters. "
                                    "Try broadening your search or checking available categories.")
            
            main_logger.info(f"Retrieved {len(rows)} available {location_level}s")
            
            return result
            
        finally:
            await conn.close()
            db_logger.debug("Database connection closed")

@mcp.tool()
@log_execution_time("get_location_hierarchy")
async def get_location_hierarchy(
    ctx: Context,
    city: Optional[str] = None,
    state: Optional[str] = None,
    district: Optional[str] = None
) -> dict:
    """
    Returns the location hierarchy and breakdown for drilling down into areas, wards, 
    and sub-locations within a city, district, or state.
    """
    
    main_logger.info(f"Getting location hierarchy for city={city}, state={state}, district={district}")
    
    async with log_async_operation("get_location_hierarchy_query", perf_logger):
        conn: asyncpg.Connection = await get_db_connection()
        
        if city:
            city_variations = normalize_city_name(city)
            city_clause = " OR ".join([f"l.city = '{var}'" for var in city_variations])
        else:
            city_clause = "1=1"
        
        where_clauses = []
        if city:
            where_clauses.append(f"({city_clause})")
        if state:
            where_clauses.append(f"l.state = '{state}'")
        if district:
            where_clauses.append(f"l.district = '{district}'")
        
        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        query = f"""
            SELECT DISTINCT
                l.state,
                l.district,
                l.city,
                l.hobli_name,
                l.grama_panchayath,
                COUNT(*) OVER (PARTITION BY l.state, l.city) as city_count,
                COUNT(*) OVER (PARTITION BY l.state, l.district) as district_count
            FROM "tabLocation" l
            WHERE {where_clause}
            ORDER BY l.state, l.city, l.district, l.hobli_name
            LIMIT 100
        """
        
        db_logger.info(f"Executing hierarchy query")
        
        try:
            async with db_perf_logger.log_query(query, operation_type="SELECT"):
                rows = await conn.fetch(query)
            
            await insert_query("get_location_hierarchy", {
                "city": city,
                "state": state,
                "district": district
            })
            
            hierarchy = {
                "query_location": {"city": city, "state": state, "district": district},
                "available_subdivisions": [],
                "summary": {}
            }
            
            hoblis = set()
            panchayaths = set()
            cities = set()
            districts = set()
            
            for row in rows:
                if row["hobli_name"]:
                    hoblis.add(row["hobli_name"])
                if row["grama_panchayath"]:
                    panchayaths.add(row["grama_panchayath"])
                if row["city"]:
                    cities.add(row["city"])
                if row["district"]:
                    districts.add(row["district"])
            
            if hoblis:
                hierarchy["available_subdivisions"].append({
                    "type": "hobli",
                    "count": len(hoblis),
                    "examples": list(hoblis)[:10]
                })
            
            if panchayaths:
                hierarchy["available_subdivisions"].append({
                    "type": "grama_panchayath",
                    "count": len(panchayaths),
                    "examples": list(panchayaths)[:10]
                })
            
            hierarchy["summary"] = {
                "total_sub_locations": len(rows),
                "cities": len(cities),
                "districts": len(districts),
                "hoblis": len(hoblis),
                "grama_panchayaths": len(panchayaths)
            }
            
            if len(rows) == 0:
                hierarchy["message"] = ("No sub-location data found for the specified location. "
                                       "The location might not have detailed geographic data, "
                                       "or the location name may need to be adjusted.")
            else:
                hierarchy["message"] = (f"Found {len(rows)} sub-locations. "
                                       "You can query data at these more granular levels.")
            
            main_logger.info(f"Retrieved location hierarchy with {len(rows)} entries")
            
            return hierarchy
            
        finally:
            await conn.close()
            db_logger.debug("Database connection closed")

# ==================== DATA QUERY TOOLS ====================

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
        
        # ENHANCED: Use city name normalization
        if city:
            city_variations = normalize_city_name(city)
            city_clause = " OR ".join([f"em.city = '{var}'" for var in city_variations])
            where_clauses.append(f"({city_clause})")
        
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
    URLs in responses are automatically formatted as clickable markdown links.
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
                    
                    # ENHANCED: Format URLs in example values
                    formatted_examples = [
                        format_urls_as_markdown_links(str(val)) if val else val
                        for val in example_values
                    ]
                    
                    main_logger.debug(f"Field {field_name}: {len(example_values)} examples")

                    rows.append(
                        {
                            "field_name": field_row["field_name"],
                            "field_definition": format_urls_as_markdown_links(
                                field_row["field_definition"]
                            ) if field_row["field_definition"] else None,
                            "example_values": formatted_examples,
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

# ==================== PLOTTING TOOLS ====================

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
            with log_sync_operation("input_validation", main_logger):
                if not data:
                    raise ValueError("Data cannot be empty")

                if not isinstance(data, list):
                    raise ValueError("Data must be a list of string values")

                filtered_data = [
                    str(item).strip() for item in data if item is not None and str(item).strip()
                ]

                if not filtered_data:
                    raise ValueError("No valid data found after filtering empty values")
                
                main_logger.info(f"Data validation complete: {len(filtered_data)} valid items from {len(data)} input items")

            with log_sync_operation("data_processing", main_logger):
                value_counts = Counter(filtered_data)
                labels = list(value_counts.keys())
                values = list(value_counts.values())
                
                main_logger.info(f"Data processed: {len(labels)} unique categories")

            with log_sync_operation("matplotlib_figure_creation", perf_logger):
                fig, ax = plt.subplots(figsize=(width, height))
                main_logger.debug(f"Created matplotlib figure: {width}x{height}")

            with log_sync_operation("percentage_calculation", main_logger):
                total = sum(values)
                percentages = [(value / total * 100) for value in values]

                legend_labels = []
                for i, (label, pct) in enumerate(zip(labels, percentages)):
                    if show_percentages:
                        legend_labels.append(f"{label} ({pct:.1f}%)")
                    else:
                        legend_labels.append(label)

            with log_sync_operation("pie_chart_rendering", perf_logger):
                def autopct_func(pct):
                    if pct < 5.0:
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
                
                img_buffer.seek(0, io.SEEK_END)
                buffer_size = img_buffer.tell()
                img_buffer.seek(0)
                
                main_logger.info(f"Image buffer created: {buffer_size / 1024:.2f} KB")

            with log_sync_operation("matplotlib_cleanup", main_logger):
                plt.close(fig)
                main_logger.debug("Matplotlib figure closed and memory freed")

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

            async with log_async_operation("s3_upload_pie_chart", perf_logger):
                upload_result = upload_image_to_s3(img_buffer)
                
                if upload_result.get("success"):
                    main_logger.info(f"Pie chart uploaded successfully: {upload_result['public_url']}")
                else:
                    main_logger.error(f"Pie chart upload failed: {upload_result.get('error')}")

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
            with log_sync_operation("bar_chart_validation", main_logger):
                if not data or not isinstance(data, dict):
                    raise ValueError("Data must be a non-empty dictionary")

            with log_sync_operation("bar_chart_data_processing", main_logger):
                if "raw_data" in data:
                    if isinstance(data["raw_data"], list) and data["raw_data"]:
                        if isinstance(data["raw_data"][0], dict):
                            df = pd.DataFrame(data["raw_data"])
                            if "category" not in df.columns:
                                raise ValueError(
                                    "Raw data with dictionaries must have 'category' column"
                                )

                            if "group" in df.columns and "value" in df.columns:
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
                                    counts = df["category"].value_counts().sort_index()
                                    categories = list(counts.index)
                                    series_data = {"Count": list(counts.values)}
                        else:
                            value_counts = Counter(data["raw_data"])
                            categories = list(value_counts.keys())
                            series_data = {"Count": list(value_counts.values())}
                else:
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

            with log_sync_operation("bar_chart_figure_creation", perf_logger):
                fig, ax = plt.subplots(figsize=(width, height))

            with log_sync_operation(f"bar_chart_rendering_{chart_style}", perf_logger):
                if colors is None:
                    colors = plt.cm.Set3(range(len(series_data)))
                elif len(colors) < len(series_data):
                    default_colors = plt.cm.Set3(range(len(series_data)))
                    colors.extend(default_colors[len(colors) :])

                if chart_style == "horizontal":
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
                                if value > 0:
                                    ax.text(
                                        bar.get_x() + bar.get_width() / 2,
                                        bottom + value / 2,
                                        f"{value}",
                                        ha="center",
                                        va="center",
                                        fontsize=8,
                                    )

                        bottoms = [b + v for b, v in zip(bottoms, values)]

                    ax.legend()
                    ax.set_xticks(x_pos)
                    ax.set_xticklabels(categories, rotation=rotation)
                    if x_label:
                        ax.set_xlabel(x_label)
                    if y_label:
                        ax.set_ylabel(y_label)

                else:  # grouped
                    x_pos = range(len(categories))
                    if len(series_data) == 1:
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

            with log_sync_operation("bar_chart_styling", main_logger):
                if title:
                    ax.set_title(title, fontsize=16, fontweight="bold", pad=20)
                plt.tight_layout()

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
                
                img_buffer.seek(0, io.SEEK_END)
                buffer_size = img_buffer.tell()
                img_buffer.seek(0)
                
                main_logger.info(f"Bar chart buffer created: {buffer_size / 1024:.2f} KB")

            with log_sync_operation("bar_chart_cleanup", main_logger):
                plt.close(fig)

            data_summary = {
                "chart_style": chart_style,
                "categories": categories,
                "series_count": len(series_data),
                "series_names": list(series_data.keys()),
                "total_data_points": sum(len(values) for values in series_data.values()),
            }

            async with log_async_operation("s3_upload_bar_chart", perf_logger):
                upload_result = upload_image_to_s3(img_buffer)

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
            
            performance_metrics.record_metric("bar_chart_errors", 1, "count")
            
            return {
                "error": error_msg,
                "s3_bucket": None,
                "s3_key": None,
                "public_url": None,
            }

# Add these plotting tools to your server.py file after create_bar_chart and before if __name__ == "__main__":

@mcp.tool()
@log_execution_time("create_line_plot", perf_logger)
async def create_line_plot(
    ctx: Context,
    data: dict,
    title: Optional[str] = None,
    x_label: Optional[str] = None,
    y_label: Optional[str] = None,
    colors: Optional[list[str]] = None,
    width: Optional[int] = 12,
    height: Optional[int] = 8,
    line_style: Optional[Literal["solid", "dashed", "dotted", "dashdot"]] = "solid",
    marker_style: Optional[str] = "o",
    show_markers: Optional[bool] = True,
    show_grid: Optional[bool] = True,
    smooth_lines: Optional[bool] = False,
    fill_area: Optional[bool] = False,
) -> dict:
    """Creates a line plot image and returns the public URL of the image."""
    
    main_logger.info(f"Creating line plot with {len(data) if data else 0} data elements")
    
    async with log_async_operation("line_plot_creation", perf_logger, include_memory=True):
        await insert_query(
            "create_line_plot",
            {
                "data": data,
                "title": title,
                "x_label": x_label,
                "y_label": y_label,
                "colors": colors,
                "width": width,
                "height": height,
                "line_style": line_style,
                "marker_style": marker_style,
                "show_markers": show_markers,
                "show_grid": show_grid,
                "smooth_lines": smooth_lines,
                "fill_area": fill_area,
            },
        )

        try:
            with log_sync_operation("line_plot_validation", main_logger):
                if not data or not isinstance(data, dict):
                    raise ValueError("Data must be a non-empty dictionary")

            with log_sync_operation("line_plot_data_processing", main_logger):
                x_data = None
                series_data = {}

                if "raw_data" in data:
                    if isinstance(data["raw_data"], list) and data["raw_data"]:
                        if isinstance(data["raw_data"][0], dict):
                            df = pd.DataFrame(data["raw_data"])
                            if "x" in df.columns and "y" in df.columns:
                                x_data = list(df["x"])
                                series_data["values"] = list(df["y"])
                            else:
                                raise ValueError(
                                    "Raw data with dictionaries must have 'x' and 'y' columns"
                                )
                        else:
                            raise ValueError("Raw data must be list of dictionaries")
                else:
                    x_keys = ["x", "dates", "time", "months", "years", "categories"]
                    for key in x_keys:
                        if key in data:
                            x_data = data[key]
                            break

                    if x_data is None:
                        raise ValueError(
                            "Data must contain x-axis data (x, dates, time, months, years, or categories)"
                        )

                    for key, values in data.items():
                        if key not in x_keys and isinstance(values, list):
                            if len(values) != len(x_data):
                                raise ValueError(
                                    f"Series '{key}' length ({len(values)}) doesn't match x-axis length ({len(x_data)})"
                                )
                            series_data[key] = values

                if not x_data or not series_data:
                    raise ValueError("No valid data found for plotting")
                
                main_logger.info(f"Data processed: {len(x_data)} points, {len(series_data)} series")

            with log_sync_operation("line_plot_figure_creation", perf_logger):
                fig, ax = plt.subplots(figsize=(width, height))

            with log_sync_operation("line_plot_rendering", perf_logger):
                if colors is None:
                    colors = plt.cm.tab10(range(len(series_data)))
                elif len(colors) < len(series_data):
                    default_colors = plt.cm.tab10(range(len(series_data)))
                    colors.extend(default_colors[len(colors) :])

                line_styles = {"solid": "-", "dashed": "--", "dotted": ":", "dashdot": "-."}
                ls = line_styles.get(line_style, "-")

                for i, (series_name, y_values) in enumerate(series_data.items()):
                    x_plot = list(range(len(x_data))) if isinstance(x_data[0], str) else x_data
                    y_plot = y_values

                    if smooth_lines and SCIPY_AVAILABLE and len(x_plot) > 3:
                        try:
                            x_smooth = np.linspace(min(x_plot), max(x_plot), len(x_plot) * 3)
                            x_array = np.array(x_plot)
                            y_array = np.array(y_plot)

                            sort_idx = np.argsort(x_array)
                            x_sorted = x_array[sort_idx]
                            y_sorted = y_array[sort_idx]

                            spl = make_interp_spline(
                                x_sorted, y_sorted, k=min(3, len(x_sorted) - 1)
                            )
                            y_smooth = spl(x_smooth)

                            ax.plot(
                                x_smooth,
                                y_smooth,
                                linestyle=ls,
                                color=colors[i],
                                label=series_name,
                                linewidth=2,
                                alpha=0.8,
                            )

                            if show_markers:
                                ax.scatter(
                                    x_plot,
                                    y_plot,
                                    marker=marker_style,
                                    color=colors[i],
                                    s=50,
                                    zorder=5,
                                    alpha=0.9,
                                )
                        except Exception:
                            ax.plot(
                                x_plot,
                                y_plot,
                                linestyle=ls,
                                marker=marker_style if show_markers else None,
                                color=colors[i],
                                label=series_name,
                                linewidth=2,
                                markersize=6,
                            )
                    else:
                        ax.plot(
                            x_plot,
                            y_plot,
                            linestyle=ls,
                            marker=marker_style if show_markers else None,
                            color=colors[i],
                            label=series_name,
                            linewidth=2,
                            markersize=6,
                        )

                    if fill_area:
                        ax.fill_between(x_plot, y_plot, alpha=0.3, color=colors[i])

                if isinstance(x_data[0], str):
                    ax.set_xticks(range(len(x_data)))
                    ax.set_xticklabels(
                        x_data, rotation=45 if len(max(x_data, key=len)) > 8 else 0
                    )

                if len(series_data) > 1:
                    ax.legend()

            with log_sync_operation("line_plot_styling", main_logger):
                if x_label:
                    ax.set_xlabel(x_label)
                if y_label:
                    ax.set_ylabel(y_label)
                if title:
                    ax.set_title(title, fontsize=16, fontweight="bold", pad=20)

                if show_grid:
                    ax.grid(True, alpha=0.3)

                plt.tight_layout()

            with log_sync_operation("line_plot_buffer_creation", perf_logger):
                img_buffer = io.BytesIO()
                plt.savefig(
                    img_buffer,
                    format="png",
                    dpi=300,
                    bbox_inches="tight",
                    facecolor="white",
                    edgecolor="none",
                )
                
                img_buffer.seek(0, io.SEEK_END)
                buffer_size = img_buffer.tell()
                img_buffer.seek(0)
                
                main_logger.info(f"Line plot buffer created: {buffer_size / 1024:.2f} KB")

            with log_sync_operation("line_plot_cleanup", main_logger):
                plt.close(fig)

            data_summary = {
                "series_count": len(series_data),
                "series_names": list(series_data.keys()),
                "data_points_per_series": len(x_data),
                "x_axis_type": "categorical" if isinstance(x_data[0], str) else "numerical",
                "smoothing_applied": smooth_lines and SCIPY_AVAILABLE,
                "area_filled": fill_area,
            }

            async with log_async_operation("s3_upload_line_plot", perf_logger):
                upload_result = upload_image_to_s3(img_buffer)

            performance_metrics.record_metric("line_plot_series", len(series_data), "count")
            performance_metrics.record_metric("line_plot_points", len(x_data), "count")
            performance_metrics.record_metric("line_plot_size", buffer_size / 1024, "KB")

            return {
                "public_url": upload_result["public_url"],
                "data_summary": data_summary,
                "title": title or "Line Plot",
                "instructions": f"The line plot has been uploaded to S3 at {upload_result['public_url']}. The data_summary provides details about the chart configuration and data.",
            }

        except Exception as e:
            error_msg = f"Failed to create line plot: {str(e)}"
            main_logger.error(error_msg)
            await ctx.debug(f"Error creating line plot: {str(e)}")
            
            performance_metrics.record_metric("line_plot_errors", 1, "count")
            
            return {
                "error": error_msg,
                "s3_bucket": None,
                "s3_key": None,
                "public_url": None,
            }

@mcp.tool()
@log_execution_time("create_histogram", perf_logger)
async def create_histogram(
    ctx: Context,
    data: list[float],
    title: Optional[str] = None,
    x_label: Optional[str] = None,
    y_label: Optional[str] = None,
    bins: Optional[int] = 30,
    color: Optional[str] = "skyblue",
    width: Optional[int] = 10,
    height: Optional[int] = 8,
    show_density: Optional[bool] = False,
    show_stats: Optional[bool] = True,
    alpha: Optional[float] = 0.7,
) -> dict:
    """Creates a histogram image and returns the public URL of the image."""
    
    main_logger.info(f"Creating histogram with {len(data) if data else 0} data points")
    
    async with log_async_operation("histogram_creation", perf_logger, include_memory=True):
        await insert_query(
            "create_histogram",
            {
                "data": data,
                "title": title,
                "x_label": x_label,
                "y_label": y_label,
                "bins": bins,
                "color": color,
                "width": width,
                "height": height,
                "show_density": show_density,
                "show_stats": show_stats,
                "alpha": alpha,
            },
        )

        try:
            with log_sync_operation("histogram_validation", main_logger):
                if not data:
                    raise ValueError("Data cannot be empty")

                if not isinstance(data, list):
                    raise ValueError("Data must be a list of numerical values")

                filtered_data = []
                for item in data:
                    if item is not None:
                        try:
                            filtered_data.append(float(item))
                        except (ValueError, TypeError):
                            pass

                if not filtered_data:
                    raise ValueError("No valid numerical data found")
                
                main_logger.info(f"Data validation complete: {len(filtered_data)} valid items")

            with log_sync_operation("histogram_statistics", main_logger):
                data_array = np.array(filtered_data)

                stats = {
                    "count": len(filtered_data),
                    "mean": float(np.mean(data_array)),
                    "median": float(np.median(data_array)),
                    "std": float(np.std(data_array)),
                    "min": float(np.min(data_array)),
                    "max": float(np.max(data_array)),
                    "q25": float(np.percentile(data_array, 25)),
                    "q75": float(np.percentile(data_array, 75)),
                }
                
                main_logger.debug(f"Statistics calculated: mean={stats['mean']:.2f}, std={stats['std']:.2f}")

            with log_sync_operation("histogram_figure_creation", perf_logger):
                fig, ax = plt.subplots(figsize=(width, height))

            with log_sync_operation("histogram_rendering", perf_logger):
                n, bins_edges, patches = ax.hist(
                    filtered_data,
                    bins=bins,
                    color=color,
                    alpha=alpha,
                    density=show_density,
                    edgecolor="black",
                    linewidth=0.5,
                )

                if x_label:
                    ax.set_xlabel(x_label, fontsize=12)
                if y_label:
                    ax.set_ylabel(y_label, fontsize=12)
                elif show_density:
                    ax.set_ylabel("Density", fontsize=12)
                else:
                    ax.set_ylabel("Frequency", fontsize=12)

                if title:
                    ax.set_title(title, fontsize=16, fontweight="bold", pad=20)

                if show_stats:
                    stats_text = f"""Statistics:
Count: {stats['count']:,}
Mean: {stats['mean']:.2f}
Median: {stats['median']:.2f}
Std Dev: {stats['std']:.2f}
Range: {stats['min']:.2f} - {stats['max']:.2f}"""

                    ax.text(
                        0.98,
                        0.98,
                        stats_text,
                        transform=ax.transAxes,
                        verticalalignment="top",
                        horizontalalignment="right",
                        bbox=dict(boxstyle="round", facecolor="white", alpha=0.8),
                        fontsize=9,
                        fontfamily="monospace",
                    )

                ax.axvline(
                    stats["mean"],
                    color="red",
                    linestyle="--",
                    linewidth=2,
                    label=f'Mean: {stats["mean"]:.2f}',
                    alpha=0.8,
                )

                ax.axvline(
                    stats["median"],
                    color="green",
                    linestyle="--",
                    linewidth=2,
                    label=f'Median: {stats["median"]:.2f}',
                    alpha=0.8,
                )

                ax.legend()
                ax.grid(True, alpha=0.3)
                plt.tight_layout()

            with log_sync_operation("histogram_buffer_creation", perf_logger):
                img_buffer = io.BytesIO()
                plt.savefig(
                    img_buffer,
                    format="png",
                    dpi=300,
                    bbox_inches="tight",
                    facecolor="white",
                    edgecolor="none",
                )
                
                img_buffer.seek(0, io.SEEK_END)
                buffer_size = img_buffer.tell()
                img_buffer.seek(0)
                
                main_logger.info(f"Histogram buffer created: {buffer_size / 1024:.2f} KB")

            with log_sync_operation("histogram_cleanup", main_logger):
                plt.close(fig)

            data_summary = {
                "statistics": stats,
                "bins_used": len(bins_edges) - 1,
                "density_mode": show_density,
                "data_range": stats["max"] - stats["min"],
                "bin_width": (stats["max"] - stats["min"]) / bins if bins > 0 else 0,
            }

            async with log_async_operation("s3_upload_histogram", perf_logger):
                upload_result = upload_image_to_s3(img_buffer)

            performance_metrics.record_metric("histogram_data_points", len(filtered_data), "count")
            performance_metrics.record_metric("histogram_bins", bins, "count")
            performance_metrics.record_metric("histogram_size", buffer_size / 1024, "KB")

            return {
                "public_url": upload_result["public_url"],
                "data_summary": data_summary,
                "title": title or "Histogram",
                "instructions": f"The histogram has been uploaded to S3 at {upload_result['public_url']}. The data_summary includes detailed statistics about the distribution.",
            }

        except Exception as e:
            error_msg = f"Failed to create histogram: {str(e)}"
            main_logger.error(error_msg)
            await ctx.debug(f"Error creating histogram: {str(e)}")
            
            performance_metrics.record_metric("histogram_errors", 1, "count")
            
            return {
                "error": error_msg,
                "s3_bucket": None,
                "s3_key": None,
                "public_url": None,
            }

@mcp.tool()
@log_execution_time("create_box_plot", perf_logger)
async def create_box_plot(
    ctx: Context,
    data: dict,
    title: Optional[str] = None,
    x_label: Optional[str] = None,
    y_label: Optional[str] = None,
    colors: Optional[list[str]] = None,
    width: Optional[int] = 10,
    height: Optional[int] = 8,
    show_outliers: Optional[bool] = True,
    show_means: Optional[bool] = True,
    orientation: Optional[Literal["vertical", "horizontal"]] = "vertical",
) -> dict:
    """Creates a box plot image and returns the public URL of the image."""
    
    main_logger.info(f"Creating {orientation} box plot with {len(data) if data else 0} groups")
    
    async with log_async_operation("box_plot_creation", perf_logger, include_memory=True):
        await insert_query(
            "create_box_plot",
            {
                "data": data,
                "title": title,
                "x_label": x_label,
                "y_label": y_label,
                "colors": colors,
                "width": width,
                "height": height,
                "show_outliers": show_outliers,
                "show_means": show_means,
                "orientation": orientation,
            },
        )

        try:
            with log_sync_operation("box_plot_validation", main_logger):
                if not data or not isinstance(data, dict):
                    raise ValueError("Data must be a non-empty dictionary")

            with log_sync_operation("box_plot_data_processing", main_logger):
                plot_data = []
                labels = []

                if "raw_data" in data:
                    if isinstance(data["raw_data"], list) and data["raw_data"]:
                        if isinstance(data["raw_data"][0], dict):
                            df = pd.DataFrame(data["raw_data"])
                            if "category" not in df.columns or "value" not in df.columns:
                                raise ValueError(
                                    "Raw data must have 'category' and 'value' columns"
                                )

                            for category in df["category"].unique():
                                category_data = df[df["category"] == category]["value"].tolist()
                                numeric_data = []
                                for val in category_data:
                                    try:
                                        numeric_data.append(float(val))
                                    except (ValueError, TypeError):
                                        pass
                                if numeric_data:
                                    plot_data.append(numeric_data)
                                    labels.append(str(category))
                        else:
                            raise ValueError("Raw data must be list of dictionaries")
                else:
                    for key, values in data.items():
                        if isinstance(values, list) and values:
                            numeric_data = []
                            for val in values:
                                try:
                                    numeric_data.append(float(val))
                                except (ValueError, TypeError):
                                    pass
                            if numeric_data:
                                plot_data.append(numeric_data)
                                labels.append(str(key))

                if not plot_data:
                    raise ValueError("No valid numerical data found for plotting")
                
                main_logger.info(f"Data processed: {len(plot_data)} groups")

            with log_sync_operation("box_plot_figure_creation", perf_logger):
                fig, ax = plt.subplots(figsize=(width, height))

            with log_sync_operation("box_plot_rendering", perf_logger):
                box_plot = ax.boxplot(
                    plot_data,
                    labels=labels,
                    patch_artist=True,
                    showmeans=show_means,
                    showfliers=show_outliers,
                    vert=(orientation == "vertical"),
                )

                if colors is None:
                    colors = plt.cm.Set3(range(len(plot_data)))
                elif len(colors) < len(plot_data):
                    default_colors = plt.cm.Set3(range(len(plot_data)))
                    colors.extend(default_colors[len(colors) :])

                for patch, color in zip(box_plot["boxes"], colors):
                    patch.set_facecolor(color)
                    patch.set_alpha(0.7)

                for element in ["whiskers", "fliers", "medians", "caps"]:
                    plt.setp(box_plot[element], color="black")

                if show_means:
                    plt.setp(
                        box_plot["means"],
                        marker="D",
                        markerfacecolor="red",
                        markeredgecolor="red",
                    )

            with log_sync_operation("box_plot_styling", main_logger):
                if orientation == "vertical":
                    if x_label:
                        ax.set_xlabel(x_label)
                    if y_label:
                        ax.set_ylabel(y_label)
                else:
                    if x_label:
                        ax.set_ylabel(x_label)
                    if y_label:
                        ax.set_xlabel(y_label)

                if title:
                    ax.set_title(title, fontsize=16, fontweight="bold", pad=20)

                ax.grid(True, alpha=0.3)
                plt.tight_layout()

            with log_sync_operation("box_plot_buffer_creation", perf_logger):
                img_buffer = io.BytesIO()
                plt.savefig(
                    img_buffer,
                    format="png",
                    dpi=300,
                    bbox_inches="tight",
                    facecolor="white",
                    edgecolor="none",
                )
                
                img_buffer.seek(0, io.SEEK_END)
                buffer_size = img_buffer.tell()
                img_buffer.seek(0)
                
                main_logger.info(f"Box plot buffer created: {buffer_size / 1024:.2f} KB")

            with log_sync_operation("box_plot_cleanup", main_logger):
                plt.close(fig)

            with log_sync_operation("box_plot_statistics", main_logger):
                group_stats = []
                for i, (label, group_data) in enumerate(zip(labels, plot_data)):
                    group_array = np.array(group_data)
                    stats = {
                        "group": label,
                        "count": len(group_data),
                        "mean": float(np.mean(group_array)),
                        "median": float(np.median(group_array)),
                        "q25": float(np.percentile(group_array, 25)),
                        "q75": float(np.percentile(group_array, 75)),
                        "min": float(np.min(group_array)),
                        "max": float(np.max(group_array)),
                        "std": float(np.std(group_array)),
                    }
                    group_stats.append(stats)

            data_summary = {
                "groups_count": len(labels),
                "group_names": labels,
                "orientation": orientation,
                "statistics": group_stats,
                "outliers_shown": show_outliers,
                "means_shown": show_means,
            }

            async with log_async_operation("s3_upload_box_plot", perf_logger):
                upload_result = upload_image_to_s3(img_buffer)

            performance_metrics.record_metric("box_plot_groups", len(labels), "count")
            performance_metrics.record_metric("box_plot_size", buffer_size / 1024, "KB")

            return {
                "public_url": upload_result["public_url"],
                "data_summary": data_summary,
                "title": title or "Box Plot",
                "instructions": f"The box plot has been uploaded to S3 at {upload_result['public_url']}. The data_summary includes statistics for each group.",
            }

        except Exception as e:
            error_msg = f"Failed to create box plot: {str(e)}"
            main_logger.error(error_msg)
            await ctx.debug(f"Error creating box plot: {str(e)}")
            
            performance_metrics.record_metric("box_plot_errors", 1, "count")
            
            return {
                "error": error_msg,
                "s3_bucket": None,
                "s3_key": None,
                "public_url": None,
            }

@mcp.tool()
@log_execution_time("create_scatter_plot", perf_logger)
async def create_scatter_plot(
    ctx: Context,
    data: dict,
    title: Optional[str] = None,
    x_label: Optional[str] = None,
    y_label: Optional[str] = None,
    colors: Optional[list[str]] = None,
    width: Optional[int] = 10,
    height: Optional[int] = 8,
    marker_size: Optional[int] = 50,
    marker_style: Optional[str] = "o",
    show_regression: Optional[bool] = False,
    show_correlation: Optional[bool] = True,
    alpha: Optional[float] = 0.7,
) -> dict:
    """Creates a scatter plot image and returns the public URL of the image."""
    
    main_logger.info(f"Creating scatter plot with regression={show_regression}")
    
    async with log_async_operation("scatter_plot_creation", perf_logger, include_memory=True):
        await insert_query(
            "create_scatter_plot",
            {
                "data": data,
                "title": title,
                "x_label": x_label,
                "y_label": y_label,
                "colors": colors,
                "width": width,
                "height": height,
                "marker_size": marker_size,
                "marker_style": marker_style,
                "show_regression": show_regression,
                "show_correlation": show_correlation,
                "alpha": alpha,
            },
        )

        try:
            with log_sync_operation("scatter_plot_validation", main_logger):
                if not data or not isinstance(data, dict):
                    raise ValueError("Data must be a non-empty dictionary")

            with log_sync_operation("scatter_plot_data_processing", main_logger):
                x_data = []
                y_data = []
                group_data = []
                size_data = []

                if "raw_data" in data:
                    if isinstance(data["raw_data"], list) and data["raw_data"]:
                        if isinstance(data["raw_data"][0], dict):
                            df = pd.DataFrame(data["raw_data"])
                            if "x" not in df.columns or "y" not in df.columns:
                                raise ValueError("Raw data must have 'x' and 'y' columns")

                            x_data = df["x"].tolist()
                            y_data = df["y"].tolist()

                            if "group" in df.columns:
                                group_data = df["group"].tolist()

                            if "size" in df.columns:
                                size_data = df["size"].tolist()
                        else:
                            raise ValueError("Raw data must be list of dictionaries")
                else:
                    if "x" not in data or "y" not in data:
                        raise ValueError("Data must contain 'x' and 'y' keys")

                    x_data = data["x"]
                    y_data = data["y"]

                    if "group" in data:
                        group_data = data["group"]

                    if "size" in data:
                        size_data = data["size"]

                if len(x_data) != len(y_data):
                    raise ValueError("x and y data must have the same length")

                if group_data and len(group_data) != len(x_data):
                    raise ValueError("group data must have the same length as x and y")

                if size_data and len(size_data) != len(x_data):
                    raise ValueError("size data must have the same length as x and y")

                x_numeric = []
                y_numeric = []
                valid_indices = []

                for i, (x_val, y_val) in enumerate(zip(x_data, y_data)):
                    try:
                        x_num = float(x_val)
                        y_num = float(y_val)
                        x_numeric.append(x_num)
                        y_numeric.append(y_num)
                        valid_indices.append(i)
                    except (ValueError, TypeError):
                        pass

                if not x_numeric:
                    raise ValueError("No valid numerical data found")

                if group_data:
                    group_data = [group_data[i] for i in valid_indices]
                if size_data:
                    size_data = [
                        float(size_data[i]) for i in valid_indices if size_data[i] is not None
                    ]
                
                main_logger.info(f"Data processed: {len(x_numeric)} valid points")

            with log_sync_operation("scatter_plot_figure_creation", perf_logger):
                fig, ax = plt.subplots(figsize=(width, height))

            with log_sync_operation("scatter_plot_rendering", perf_logger):
                if group_data:
                    unique_groups = list(set(group_data))

                    if colors is None:
                        colors = plt.cm.tab10(range(len(unique_groups)))
                    elif len(colors) < len(unique_groups):
                        default_colors = plt.cm.tab10(range(len(unique_groups)))
                        colors.extend(default_colors[len(colors) :])

                    for i, group in enumerate(unique_groups):
                        group_indices = [j for j, g in enumerate(group_data) if g == group]
                        group_x = [x_numeric[j] for j in group_indices]
                        group_y = [y_numeric[j] for j in group_indices]

                        if size_data:
                            group_sizes = [
                                size_data[j] for j in group_indices if j < len(size_data)
                            ]
                            if len(group_sizes) != len(group_x):
                                group_sizes = [marker_size] * len(group_x)
                        else:
                            group_sizes = marker_size

                        ax.scatter(
                            group_x,
                            group_y,
                            s=group_sizes,
                            c=[colors[i]],
                            marker=marker_style,
                            alpha=alpha,
                            label=str(group),
                        )

                    ax.legend()
                else:
                    scatter_sizes = size_data if size_data else marker_size
                    color = colors[0] if colors is not None else "blue"

                    ax.scatter(
                        x_numeric,
                        y_numeric,
                        s=scatter_sizes,
                        c=color,
                        marker=marker_style,
                        alpha=alpha,
                    )

                if show_regression and SCIPY_AVAILABLE:
                    try:
                        slope, intercept, r_value, p_value, std_err = stats.linregress(
                            x_numeric, y_numeric
                        )
                        line_x = np.array([min(x_numeric), max(x_numeric)])
                        line_y = slope * line_x + intercept
                        ax.plot(
                            line_x,
                            line_y,
                            "r-",
                            linewidth=2,
                            alpha=0.8,
                            label=f"Regression (R² = {r_value**2:.3f})",
                        )
                        ax.legend()
                    except Exception:
                        pass

                correlation = None
                if show_correlation:
                    correlation = float(np.corrcoef(x_numeric, y_numeric)[0, 1])
                    ax.text(
                        0.05,
                        0.95,
                        f"Correlation: {correlation:.3f}",
                        transform=ax.transAxes,
                        fontsize=12,
                        bbox=dict(boxstyle="round", facecolor="white", alpha=0.8),
                    )

            with log_sync_operation("scatter_plot_styling", main_logger):
                if x_label:
                    ax.set_xlabel(x_label)
                if y_label:
                    ax.set_ylabel(y_label)
                if title:
                    ax.set_title(title, fontsize=16, fontweight="bold", pad=20)

                ax.grid(True, alpha=0.3)
                plt.tight_layout()

            with log_sync_operation("scatter_plot_buffer_creation", perf_logger):
                img_buffer = io.BytesIO()
                plt.savefig(
                    img_buffer,
                    format="png",
                    dpi=300,
                    bbox_inches="tight",
                    facecolor="white",
                    edgecolor="none",
                )
                
                img_buffer.seek(0, io.SEEK_END)
                buffer_size = img_buffer.tell()
                img_buffer.seek(0)
                
                main_logger.info(f"Scatter plot buffer created: {buffer_size / 1024:.2f} KB")

            with log_sync_operation("scatter_plot_cleanup", main_logger):
                plt.close(fig)

            data_summary = {
                "data_points": len(x_numeric),
                "groups": len(set(group_data)) if group_data else 1,
                "correlation": correlation,
                "x_range": [min(x_numeric), max(x_numeric)],
                "y_range": [min(y_numeric), max(y_numeric)],
                "regression_shown": show_regression and SCIPY_AVAILABLE,
            }

            async with log_async_operation("s3_upload_scatter_plot", perf_logger):
                upload_result = upload_image_to_s3(img_buffer)

            performance_metrics.record_metric("scatter_plot_points", len(x_numeric), "count")
            performance_metrics.record_metric("scatter_plot_size", buffer_size / 1024, "KB")
            if correlation:
                performance_metrics.record_metric("scatter_plot_correlation", abs(correlation), "value")

            return {
                "public_url": upload_result["public_url"],
                "data_summary": data_summary,
                "title": title or "Scatter Plot",
                "instructions": f"The scatter plot has been uploaded to S3 at {upload_result['public_url']}. The data_summary includes correlation and range information.",
            }

        except Exception as e:
            error_msg = f"Failed to create scatter plot: {str(e)}"
            main_logger.error(error_msg)
            await ctx.debug(f"Error creating scatter plot: {str(e)}")
            
            performance_metrics.record_metric("scatter_plot_errors", 1, "count")
            
            return {
                "error": error_msg,
                "s3_bucket": None,
                "s3_key": None,
                "public_url": None,
            }

@mcp.tool()
@log_execution_time("create_donut_chart", perf_logger)
async def create_donut_chart(
    ctx: Context,
    data: list[str],
    title: Optional[str] = None,
    colors: Optional[list[str]] = None,
    width: Optional[int] = 10,
    height: Optional[int] = 8,
    show_percentages: Optional[bool] = True,
    start_angle: Optional[int] = 90,
    hole_size: Optional[float] = 0.3,
    center_text: Optional[str] = None,
) -> dict:
    """Creates a donut chart image and returns the public URL of the image."""
    
    main_logger.info(f"Creating donut chart with {len(data) if data else 0} data points, hole_size={hole_size}")
    
    async with log_async_operation("donut_chart_creation", perf_logger, include_memory=True):
        await insert_query(
            "create_donut_chart",
            {
                "data": data,
                "title": title,
                "colors": colors,
                "width": width,
                "height": height,
                "show_percentages": show_percentages,
                "start_angle": start_angle,
                "hole_size": hole_size,
                "center_text": center_text,
            },
        )

        try:
            with log_sync_operation("donut_chart_validation", main_logger):
                if not data:
                    raise ValueError("Data cannot be empty")

                if not isinstance(data, list):
                    raise ValueError("Data must be a list of string values")

                filtered_data = [
                    str(item).strip() for item in data if item is not None and str(item).strip()
                ]

                if not filtered_data:
                    raise ValueError("No valid data found after filtering empty values")
                
                main_logger.info(f"Data validation complete: {len(filtered_data)} valid items")

            with log_sync_operation("donut_chart_data_processing", main_logger):
                value_counts = Counter(filtered_data)
                labels = list(value_counts.keys())
                values = list(value_counts.values())
                
                main_logger.info(f"Data processed: {len(labels)} unique categories")

            with log_sync_operation("donut_chart_figure_creation", perf_logger):
                fig, ax = plt.subplots(figsize=(width, height))

            with log_sync_operation("donut_chart_rendering", perf_logger):
                total = sum(values)
                percentages = [(value / total * 100) for value in values]

                legend_labels = []
                for i, (label, pct) in enumerate(zip(labels, percentages)):
                    if show_percentages:
                        legend_labels.append(f"{label} ({pct:.1f}%)")
                    else:
                        legend_labels.append(label)

                def autopct_func(pct):
                    if pct < 5.0:
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
                        wedgeprops=dict(width=1 - hole_size),
                    )
                else:
                    wedges, texts = ax.pie(
                        values,
                        labels=None,
                        autopct=autopct,
                        startangle=start_angle,
                        colors=colors,
                        wedgeprops=dict(width=1 - hole_size),
                    )
                    autotexts = None

                if center_text:
                    ax.text(
                        0,
                        0,
                        center_text,
                        horizontalalignment="center",
                        verticalalignment="center",
                        fontsize=14,
                        fontweight="bold",
                    )

            with log_sync_operation("donut_chart_styling", main_logger):
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

            with log_sync_operation("donut_chart_buffer_creation", perf_logger):
                img_buffer = io.BytesIO()
                plt.savefig(
                    img_buffer,
                    format="png",
                    dpi=300,
                    bbox_inches="tight",
                    facecolor="white",
                    edgecolor="none",
                )
                
                img_buffer.seek(0, io.SEEK_END)
                buffer_size = img_buffer.tell()
                img_buffer.seek(0)
                
                main_logger.info(f"Donut chart buffer created: {buffer_size / 1024:.2f} KB")

            with log_sync_operation("donut_chart_cleanup", main_logger):
                plt.close(fig)

            data_summary = {
                "total_categories": len(labels),
                "total_items": len(filtered_data),
                "total_count": total,
                "hole_size": hole_size,
                "center_text": center_text,
                "categories": [
                    {
                        "label": label,
                        "count": count,
                        "percentage": round((count / total) * 100, 2) if total > 0 else 0,
                    }
                    for label, count in zip(labels, values)
                ],
            }

            async with log_async_operation("s3_upload_donut_chart", perf_logger):
                upload_result = upload_image_to_s3(img_buffer)

            performance_metrics.record_metric("donut_chart_categories", len(labels), "count")
            performance_metrics.record_metric("donut_chart_size", buffer_size / 1024, "KB")

            return {
                "public_url": upload_result["public_url"],
                "data_summary": data_summary,
                "title": title or "Donut Chart",
                "instructions": f"The donut chart has been uploaded to S3 at {upload_result['public_url']}. The data_summary provides details about the chart contents including counts and percentages.",
            }

        except Exception as e:
            error_msg = f"Failed to create donut chart: {str(e)}"
            main_logger.error(error_msg)
            await ctx.debug(f"Error creating donut chart: {str(e)}")
            
            performance_metrics.record_metric("donut_chart_errors", 1, "count")
            
            return {
                "error": error_msg,
                "s3_bucket": None,
                "s3_key": None,
                "public_url": None,
            }

@mcp.tool()
@log_execution_time("create_heatmap", perf_logger)
async def create_heatmap(
    ctx: Context,
    data: dict,
    title: Optional[str] = None,
    x_label: Optional[str] = None,
    y_label: Optional[str] = None,
    colormap: Optional[str] = "viridis",
    width: Optional[int] = 12,
    height: Optional[int] = 8,
    show_values: Optional[bool] = True,
    value_format: Optional[str] = ".2f",
) -> dict:
    """Creates a heatmap image and returns the public URL of the image."""
    
    main_logger.info(f"Creating heatmap with colormap={colormap}")
    
    async with log_async_operation("heatmap_creation", perf_logger, include_memory=True):
        await insert_query(
            "create_heatmap",
            {
                "data": data,
                "title": title,
                "x_label": x_label,
                "y_label": y_label,
                "colormap": colormap,
                "width": width,
                "height": height,
                "show_values": show_values,
                "value_format": value_format,
            },
        )

        try:
            with log_sync_operation("heatmap_validation", main_logger):
                if not data or not isinstance(data, dict):
                    raise ValueError("Data must be a non-empty dictionary")

            with log_sync_operation("heatmap_data_processing", main_logger):
                matrix = None
                x_labels = None
                y_labels = None

                if "matrix" in data:
                    matrix = np.array(data["matrix"])
                    x_labels = data.get(
                        "x_labels", [f"Col {i}" for i in range(matrix.shape[1])]
                    )
                    y_labels = data.get(
                        "y_labels", [f"Row {i}" for i in range(matrix.shape[0])]
                    )
                elif "raw_data" in data:
                    if isinstance(data["raw_data"], list) and data["raw_data"]:
                        if isinstance(data["raw_data"][0], dict):
                            df = pd.DataFrame(data["raw_data"])
                            if not all(col in df.columns for col in ["x", "y", "value"]):
                                raise ValueError(
                                    "Raw data must have 'x', 'y', and 'value' columns"
                                )

                            pivot_df = df.pivot_table(
                                values="value",
                                index="y",
                                columns="x",
                                aggfunc="mean",
                                fill_value=0,
                            )
                            matrix = pivot_df.values
                            x_labels = list(pivot_df.columns)
                            y_labels = list(pivot_df.index)
                        else:
                            raise ValueError("Raw data must be list of dictionaries")
                elif "x" in data and "y" in data and "values" in data:
                    df = pd.DataFrame(
                        {"x": data["x"], "y": data["y"], "values": data["values"]}
                    )
                    pivot_df = df.pivot_table(
                        values="values", index="y", columns="x", aggfunc="mean", fill_value=0
                    )
                    matrix = pivot_df.values
                    x_labels = list(pivot_df.columns)
                    y_labels = list(pivot_df.index)
                else:
                    raise ValueError(
                        "Data must contain matrix format, raw_data, or x/y/values format"
                    )

                if matrix is None:
                    raise ValueError("No valid matrix data found")
                
                main_logger.info(f"Data processed: matrix shape {matrix.shape}")

            with log_sync_operation("heatmap_figure_creation", perf_logger):
                fig, ax = plt.subplots(figsize=(width, height))

            with log_sync_operation("heatmap_rendering", perf_logger):
                im = ax.imshow(matrix, cmap=colormap, aspect="auto")

                ax.set_xticks(np.arange(len(x_labels)))
                ax.set_yticks(np.arange(len(y_labels)))
                ax.set_xticklabels(x_labels)
                ax.set_yticklabels(y_labels)

                plt.setp(ax.get_xticklabels(), rotation=45, ha="right", rotation_mode="anchor")

                cbar = ax.figure.colorbar(im, ax=ax)

                if show_values:
                    for i in range(len(y_labels)):
                        for j in range(len(x_labels)):
                            value = matrix[i, j]
                            text_color = (
                                "white"
                                if value > (matrix.max() + matrix.min()) / 2
                                else "black"
                            )
                            text = ax.text(
                                j,
                                i,
                                format(value, value_format),
                                ha="center",
                                va="center",
                                color=text_color,
                                fontsize=8,
                            )

            with log_sync_operation("heatmap_styling", main_logger):
                if x_label:
                    ax.set_xlabel(x_label)
                if y_label:
                    ax.set_ylabel(y_label)
                if title:
                    ax.set_title(title, fontsize=16, fontweight="bold", pad=20)

                plt.tight_layout()

            with log_sync_operation("heatmap_buffer_creation", perf_logger):
                img_buffer = io.BytesIO()
                plt.savefig(
                    img_buffer,
                    format="png",
                    dpi=300,
                    bbox_inches="tight",
                    facecolor="white",
                    edgecolor="none",
                )
                
                img_buffer.seek(0, io.SEEK_END)
                buffer_size = img_buffer.tell()
                img_buffer.seek(0)
                
                main_logger.info(f"Heatmap buffer created: {buffer_size / 1024:.2f} KB")

            with log_sync_operation("heatmap_cleanup", main_logger):
                plt.close(fig)

            data_summary = {
                "matrix_shape": matrix.shape,
                "x_categories": len(x_labels),
                "y_categories": len(y_labels),
                "value_range": [float(matrix.min()), float(matrix.max())],
                "mean_value": float(matrix.mean()),
                "colormap": colormap,
                "values_shown": show_values,
            }

            async with log_async_operation("s3_upload_heatmap", perf_logger):
                upload_result = upload_image_to_s3(img_buffer)

            performance_metrics.record_metric("heatmap_cells", matrix.size, "count")
            performance_metrics.record_metric("heatmap_size", buffer_size / 1024, "KB")

            return {
                "public_url": upload_result["public_url"],
                "data_summary": data_summary,
                "title": title or "Heatmap",
                "instructions": f"The heatmap has been uploaded to S3 at {upload_result['public_url']}. The data_summary provides details about the matrix dimensions and value ranges.",
            }

        except Exception as e:
            error_msg = f"Failed to create heatmap: {str(e)}"
            main_logger.error(error_msg)
            await ctx.debug(f"Error creating heatmap: {str(e)}")
            
            performance_metrics.record_metric("heatmap_errors", 1, "count")
            
            return {
                "error": error_msg,
                "s3_bucket": None,
                "s3_key": None,
                "public_url": None,
            }

@mcp.tool()
@log_execution_time("create_area_chart", perf_logger)
async def create_area_chart(
    ctx: Context,
    data: dict,
    title: Optional[str] = None,
    x_label: Optional[str] = None,
    y_label: Optional[str] = None,
    colors: Optional[list[str]] = None,
    width: Optional[int] = 12,
    height: Optional[int] = 8,
    chart_style: Optional[Literal["stacked", "overlapping"]] = "stacked",
    alpha: Optional[float] = 0.7,
    show_markers: Optional[bool] = False,
) -> dict:
    """Creates an area chart image and returns the public URL of the image."""
    
    main_logger.info(f"Creating {chart_style} area chart")
    
    async with log_async_operation("area_chart_creation", perf_logger, include_memory=True):
        await insert_query(
            "create_area_chart",
            {
                "data": data,
                "title": title,
                "x_label": x_label,
                "y_label": y_label,
                "colors": colors,
                "width": width,
                "height": height,
                "chart_style": chart_style,
                "alpha": alpha,
                "show_markers": show_markers,
            },
        )

        try:
            with log_sync_operation("area_chart_validation", main_logger):
                if not data or not isinstance(data, dict):
                    raise ValueError("Data must be a non-empty dictionary")

            with log_sync_operation("area_chart_data_processing", main_logger):
                x_data = None
                series_data = {}

                if "raw_data" in data:
                    if isinstance(data["raw_data"], list) and data["raw_data"]:
                        if isinstance(data["raw_data"][0], dict):
                            df = pd.DataFrame(data["raw_data"])
                            if not all(col in df.columns for col in ["x", "series", "value"]):
                                raise ValueError(
                                    "Raw data must have 'x', 'series', and 'value' columns"
                                )

                            pivot_df = df.pivot_table(
                                values="value",
                                index="x",
                                columns="series",
                                aggfunc="sum",
                                fill_value=0,
                            )
                            x_data = list(pivot_df.index)
                            series_data = {col: list(pivot_df[col]) for col in pivot_df.columns}
                        else:
                            raise ValueError("Raw data must be list of dictionaries")
                else:
                    x_keys = ["x", "time", "dates", "months", "years", "categories"]
                    for key in x_keys:
                        if key in data:
                            x_data = data[key]
                            break

                    if x_data is None:
                        raise ValueError(
                            "Data must contain x-axis data (x, time, dates, months, years, or categories)"
                        )

                    for key, values in data.items():
                        if key not in x_keys and isinstance(values, list):
                            if len(values) != len(x_data):
                                raise ValueError(
                                    f"Series '{key}' length ({len(values)}) doesn't match x-axis length ({len(x_data)})"
                                )
                            series_data[key] = values

                if not x_data or not series_data:
                    raise ValueError("No valid data found for plotting")
                
                main_logger.info(f"Data processed: {len(x_data)} points, {len(series_data)} series")

            with log_sync_operation("area_chart_figure_creation", perf_logger):
                fig, ax = plt.subplots(figsize=(width, height))

            with log_sync_operation("area_chart_rendering", perf_logger):
                if colors is None:
                    colors = plt.cm.Set3(range(len(series_data)))
                elif len(colors) < len(series_data):
                    default_colors = plt.cm.Set3(range(len(series_data)))
                    colors.extend(default_colors[len(colors) :])

                x_plot = list(range(len(x_data))) if isinstance(x_data[0], str) else x_data

                if chart_style == "stacked":
                    y_stacked = np.zeros(len(x_plot))

                    for i, (series_name, y_values) in enumerate(series_data.items()):
                        y_array = np.array(y_values)

                        ax.fill_between(
                            x_plot,
                            y_stacked,
                            y_stacked + y_array,
                            alpha=alpha,
                            color=colors[i],
                            label=series_name,
                        )

                        if show_markers:
                            ax.plot(
                                x_plot,
                                y_stacked + y_array,
                                marker="o",
                                markersize=4,
                                color=colors[i],
                                linewidth=1,
                            )

                        y_stacked += y_array

                else:  # overlapping
                    for i, (series_name, y_values) in enumerate(series_data.items()):
                        ax.fill_between(
                            x_plot, 0, y_values, alpha=alpha, color=colors[i], label=series_name
                        )

                        if show_markers:
                            ax.plot(
                                x_plot,
                                y_values,
                                marker="o",
                                markersize=4,
                                color=colors[i],
                                linewidth=2,
                            )

                if isinstance(x_data[0], str):
                    ax.set_xticks(range(len(x_data)))
                    ax.set_xticklabels(
                        x_data, rotation=45 if len(max(x_data, key=len)) > 8 else 0
                    )

                if len(series_data) > 1:
                    ax.legend()

            with log_sync_operation("area_chart_styling", main_logger):
                if x_label:
                    ax.set_xlabel(x_label)
                if y_label:
                    ax.set_ylabel(y_label)
                if title:
                    ax.set_title(title, fontsize=16, fontweight="bold", pad=20)

                ax.grid(True, alpha=0.3)
                plt.tight_layout()

            with log_sync_operation("area_chart_buffer_creation", perf_logger):
                img_buffer = io.BytesIO()
                plt.savefig(
                    img_buffer,
                    format="png",
                    dpi=300,
                    bbox_inches="tight",
                    facecolor="white",
                    edgecolor="none",
                )
                
                img_buffer.seek(0, io.SEEK_END)
                buffer_size = img_buffer.tell()
                img_buffer.seek(0)
                
                main_logger.info(f"Area chart buffer created: {buffer_size / 1024:.2f} KB")

            with log_sync_operation("area_chart_cleanup", main_logger):
                plt.close(fig)

            data_summary = {
                "series_count": len(series_data),
                "series_names": list(series_data.keys()),
                "data_points_per_series": len(x_data),
                "x_axis_type": "categorical" if isinstance(x_data[0], str) else "numerical",
                "chart_style": chart_style,
                "total_max": (
                    float(np.sum([max(values) for values in series_data.values()]))
                    if chart_style == "stacked"
                    else float(max([max(values) for values in series_data.values()]))
                ),
            }

            async with log_async_operation("s3_upload_area_chart", perf_logger):
                upload_result = upload_image_to_s3(img_buffer)

            performance_metrics.record_metric("area_chart_series", len(series_data), "count")
            performance_metrics.record_metric("area_chart_points", len(x_data), "count")
            performance_metrics.record_metric("area_chart_size", buffer_size / 1024, "KB")

            return {
                "public_url": upload_result["public_url"],
                "data_summary": data_summary,
                "title": title or "Area Chart",
                "instructions": f"The area chart has been uploaded to S3 at {upload_result['public_url']}. The data_summary provides details about the chart configuration and data.",
            }

        except Exception as e:
            error_msg = f"Failed to create area chart: {str(e)}"
            main_logger.error(error_msg)
            await ctx.debug(f"Error creating area chart: {str(e)}")
            
            performance_metrics.record_metric("area_chart_errors", 1, "count")
            
            return {
                "error": error_msg,
                "s3_bucket": None,
                "s3_key": None,
                "public_url": None,
            }

if __name__ == "__main__":
    main_logger.info("Starting MCP server...")
    mcp.run(transport="sse")
    main_logger.info("MCP server started successfully")