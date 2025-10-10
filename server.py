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

# ==================== CITY NAME NORMALIZATION ====================

# City name mappings for common variations
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
    """
    Returns all variations of a city name for robust database queries
    
    Args:
        city: City name to normalize
        
    Returns:
        List of all possible variations of the city name
    """
    city_lower = city.lower().strip()
    
    # Check if we have mappings for this city
    if city_lower in CITY_NAME_MAPPINGS:
        variations = CITY_NAME_MAPPINGS[city_lower]
        main_logger.debug(f"Found {len(variations)} variations for '{city}': {variations}")
        return variations
    
    # If no mapping, return the original with different casings
    variations = [city, city.title(), city.lower(), city.upper()]
    main_logger.debug(f"No predefined mapping for '{city}', using case variations")
    return list(set(variations))  # Remove duplicates

# ==================== URL FORMATTING ====================

@log_execution_time("format_urls_as_markdown_links", main_logger)
def format_urls_as_markdown_links(text: str) -> str:
    """
    Convert plain URLs to markdown links with descriptive titles
    
    Args:
        text: Text containing URLs
        
    Returns:
        Text with URLs converted to markdown format
    """
    if not text or not isinstance(text, str):
        return text
    
    # URL pattern
    url_pattern = r'(https?://[^\s<>"{}|\\^`\[\]]+)'
    
    def replace_url(match):
        url = match.group(1)
        
        # Determine URL type and create descriptive title
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
    
    # Replace all URLs with markdown links
    formatted_text = re.sub(url_pattern, replace_url, text)
    
    # Count how many URLs were formatted
    url_count = len(re.findall(url_pattern, text))
    if url_count > 0:
        main_logger.info(f"Formatted {url_count} URLs to markdown links")
    
    return formatted_text

# ==================== DATABASE CONNECTION ====================

@log_execution_time("get_db_connection", db_logger)
async def get_db_connection():
    """Get database connection with logging"""
    async with log_async_operation("postgres_connection", db_logger):
        db_logger.info(f"Establishing PostgreSQL connection to: {DATABASE_URL[:50]}...")
        conn = await asyncpg.connect(DATABASE_URL)
        db_logger.debug("PostgreSQL connection established successfully")
        return conn

# ==================== LOCATION DISCOVERY TOOLS ====================

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
    
    Args:
        category: Filter by category (optional)
        subcategory: Filter by subcategory (optional)  
        partner: Filter by partner (optional)
        location_level: Whether to return cities, states, or districts
        
    Returns:
        Dictionary with available locations and their data counts
    """
    
    main_logger.info(f"Getting available {location_level}s for category={category}, "
                    f"subcategory={subcategory}, partner={partner}")
    
    async with log_async_operation("get_available_locations_query", perf_logger):
        conn: asyncpg.Connection = await get_db_connection()
        
        # Build WHERE clauses
        where_clauses = []
        if category:
            where_clauses.append(f"em.event_category = '{category}'")
        if subcategory:
            where_clauses.append(f"em.event_subcategory = '{subcategory}'")
        if partner:
            where_clauses.append(f"em.partner = '{partner}'")
        
        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        # Query based on location level
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
    
    Use this to help users explore data at more granular levels (e.g., specific neighborhoods, wards).
    
    Args:
        city: City name (optional)
        state: State name (optional)
        district: District name (optional)
        
    Returns:
        Dictionary with location hierarchy and available sub-locations
    """
    
    main_logger.info(f"Getting location hierarchy for city={city}, state={state}, district={district}")
    
    async with log_async_operation("get_location_hierarchy_query", perf_logger):
        conn: asyncpg.Connection = await get_db_connection()
        
        # Normalize city name if provided
        if city:
            city_variations = normalize_city_name(city)
            city_clause = " OR ".join([f"l.city = '{var}'" for var in city_variations])
        else:
            city_clause = "1=1"
        
        # Build WHERE clause
        where_clauses = []
        if city:
            where_clauses.append(f"({city_clause})")
        if state:
            where_clauses.append(f"l.state = '{state}'")
        if district:
            where_clauses.append(f"l.district = '{district}'")
        
        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        # Query for location details
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
            
            # Organize results
            hierarchy = {
                "query_location": {
                    "city": city,
                    "state": state,
                    "district": district
                },
                "available_subdivisions": [],
                "summary": {}
            }
            
            # Collect unique values
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
            
            # Add to results
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

# ==================== EXISTING TOOLS (with enhancements) ====================

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
@log_execution_time("get_data_metadata_on_samaajdata")
async def get_data_metadata_on_samaajdata(
    ctx: Context,
    event_categories: list[str],
    event_subcategories: list[str],
    partners: list[str],
) -> dict:
    """
    Returns the list of fields for all the data matching the given filters on SamaajData.
    URLs in the response are automatically formatted as clickable markdown links.
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
                    
                    # Format URLs in example values
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

# Continue with other existing tools...
# (Include all other existing tools from the original server.py)
# For brevity, I'm showing the pattern - you would include all remaining tools

if __name__ == "__main__":
    main_logger.info("Starting MCP server...")
    mcp.run(transport="sse")
    main_logger.info("MCP server started successfully")