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

try:
    from scipy.interpolate import make_interp_spline
    from scipy import stats

    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
from utils import (
    upload_image_to_s3,
)

load_dotenv()

# Set matplotlib to use non-interactive backend
matplotlib.use("Agg")

DATABASE_URL = os.getenv("DATABASE_URL")

parser = argparse.ArgumentParser()
parser.add_argument("--port", action="store", type=int, default=8000)
args = parser.parse_args()
port = args.port

# port = 8000

mcp = FastMCP("SamaajData MCP server", host="0.0.0.0", port=port)


async def get_db_connection():
    conn = await asyncpg.connect(DATABASE_URL)
    return conn


@mcp.tool()
async def get_valid_categories(ctx: Context) -> list[str]:
    """
    Returns the list of valid issue/action/event categories that can be used as a filter. If the user query requires filtering for specific category of issue/action/event, use this method to get the list of valid categories to pick from
    """
    conn: asyncpg.Connection = await get_db_connection()

    query = 'SELECT * from "tabEvent Category"'
    rows = await conn.fetch(query)

    await insert_query("get_valid_categories", {})

    return [row["name"] for row in rows]


@mcp.tool()
async def get_valid_subcategories(ctx: Context) -> list[str]:
    """
    Returns the list of valid issue/action/event subcategories that can be used as a filter. If the user query requires filtering for specific subcategory of issue/action/event, use this method to get the list of valid subcategories to pick from. Only pick this if the category is also picked and the user query requires further filtering beyond category.
    """
    conn: asyncpg.Connection = await get_db_connection()

    query = 'SELECT * from "tabEvent Sub Category"'
    rows = await conn.fetch(query)

    await insert_query("get_valid_subcategories", {})

    return [row["name"] for row in rows]


@mcp.tool()
async def get_valid_event_types(ctx: Context) -> list[str]:
    """
    Returns the list of valid issue/action/event types that can be used as a filter. If the user query requires filtering for specific type of issue/action/event, use this method to get the list of valid types to pick from.
    """
    conn: asyncpg.Connection = await get_db_connection()

    query = 'SELECT * from "tabEvent Type"'
    rows = await conn.fetch(query)

    await insert_query("get_valid_event_types", {})

    return [row["name"] for row in rows]


@mcp.tool()
async def get_data_partners_list(ctx: Context) -> dict:
    """
    Returns the list of organizations that have contributed data to SamaajData along with the corresponding category and subcategory of the data they have contributed to be used for filtering the whole dataset.

    Whenever a user query is made, always begin by calling this tool to get the list of partners and their corresponding category and subcategory. This data can then be used to call the appropriate tools to get the required data with the right partner/category/subcategory filters.
    """
    conn: asyncpg.Connection = await get_db_connection()

    query = """
        SELECT DISTINCT partner, event_category, event_subcategory
        FROM "Events Metadata"
        WHERE partner IS NOT NULL AND partner <> ''
    """
    rows = await conn.fetch(query)

    await insert_query("get_data_partners_list", {})

    return {
        "result": [
            {
                "partner": row["partner"],
                "category": row["event_category"],
                "subcategory": row["event_subcategory"],
            }
            for row in rows
        ]
    }


@mcp.tool()
async def test_db_connection(ctx: Context) -> list[str]:
    conn: asyncpg.Connection = await get_db_connection()

    rows = await conn.fetch("SELECT title FROM tabEvents LIMIT 10")
    await conn.close()
    return [row["title"] for row in rows]


@mcp.tool()
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

    For any data requested for video volunteers, always apply the following base filters on top of which other filters can be applied:
    - event.subcategory = 'Citizen Initiatives'
    - (event.hours_invested = 0 OR event.hours_invested = 0.0 OR event.hours_invested = '0.000')
    - event.location IS NOT NULL

    Parameters:
        ctx: Internal MCP context (do not supply manually).
        aggregation_level: Geographic level to match ("district", "state", "hobli_name", or "grama_panchayath").
        value: Name of the area to match (e.g., "Ludhiana" if aggregation_level="district").
        start_date: (Optional, format: DD/MM/YYYY) Start date for filtering event creation.
        end_date: (Optional, format: DD/MM/YYYY) End date for filtering event creation.
        category: (Optional) Filter to include only events matching this category.
                  Use values returned by get_valid_categories().
        subcategory: (Optional) Filter to include only events matching this subcategory.
                     Use values returned by get_valid_subcategories().
        type: (Optional) Filter to include only events matching this event type.
              Use values returned by get_valid_event_types().

    Returns:
        A dictionary with the key "latlong" containing the list of tuples - [(latitude, longitude)]
    """

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

    if start_date:
        start = pd.to_datetime(start_date, dayfirst=True)
    else:
        start = datetime(2000, 1, 1)

    if end_date:
        end = pd.to_datetime(end_date, dayfirst=True)
    else:
        end = datetime.today()

    filters = [
        f"e.creation >= '{start.date().isoformat()}'",
        f"e.creation <= '{end.date().isoformat()}'",
        f"l.{aggregation_level} = '{value}'",
    ]
    if category:
        filters.append(f"e.category = '{category}'")
    if subcategory:
        filters.append(f"e.subcategory = '{subcategory}'")
    if type:
        filters.append(f"e.type = '{type}'")

    where_clause = " AND ".join(filters)

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

    rows = await conn.fetch(query)

    return {
        "latlong": [(row["latitude"], row["longitude"]) for row in rows],
        "description": "For the given query, the lat/long of the event points have been returned. You can use this to display a scatter plot on a map.",
    }


# @mcp.tool()
# async def extract_spatial_data_from_samaajdata(
#     ctx: Context,
#     start_date: Optional[str] = None,
#     end_date: Optional[str] = None,
#     category: Optional[str] = None,
#     subcategory: Optional[str] = None,
#     type: Optional[str] = None,
#     aggregate_by: Optional[
#         Literal["point", "district", "state", "hobli_name", "grama_panchayath"]
#     ] = None,
#     aggregation_value: Optional[str] = None,
# ) -> dict:
#     """
#     Returns the spatial data on all issues based on filters and desired aggregation level from Samaajdata. If aggregate_by is "point", the raw lat/lon for each issue is returned. Otherwise, the data is grouped by the aggregation level and all lower levels in the hierarchy.

#     This should be used only when the user query can be answered with the filters provided here. If the user's query requires more filters, use the get_data_metadata_on_samaajdata() tool to get the list of key-value pairs available for each event/data point and get the data field values using get_data_field_values_on_samaajdata() tool.

#     Aggregation hierarchy (from highest to lowest):
#     - state -> district -> hobli -> grama_panchayath -> point

#     When you specify an aggregation level, you get data for that level and all lower levels.
#     For example:
#     - aggregate_by="state" returns: state, district, hobli, grama_panchayath aggregations
#     - aggregate_by="district" returns: district, hobli, grama_panchayath aggregations
#     - aggregate_by="hobli_name" returns: hobli_name, grama_panchayath aggregations

#     For any data requested for video volunteers, always apply the following base filters on top of which other filters can be applied:
#     - event.subcategory = 'Citizen Initiatives'
#     - (event.hours_invested = 0 OR event.hours_invested = 0.0 OR event.hours_invested = '0.000')
#     - event.location IS NOT NULL

#     Parameters:
#         ctx: Internal MCP context (do not supply manually).
#         start_date: (Optional, format: DD/MM/YYYY) Start date for filtering events by creation date.
#         end_date: (Optional, format: DD/MM/YYYY) End date for filtering events by creation date.
#         category: (Optional) Filter to include only events matching this category. If given, the value must be one of the values returned by get_valid_categories(). Only pick the most relevant category.
#         subcategory: (Optional) Filter to include only events matching this subcategory. If given, the value must be one of the values returned by get_valid_subcategories(). Only pick the most relevant subcategory. Only pick this if the category is also picked and the user query requires further filtering beyond category.
#         type: (Optional) Filter to include only events matching this event type. If given, the value must be one of the values returned by get_valid_event_types(). Only pick the most relevant type.
#         aggregate_by: (Optional) How to group events for clustering.
#             - "point": raw lat/lon for each issue (no aggregation)
#             - "state": grouped by state and all lower levels (district, hobli, grama_panchayath)
#             - "district": grouped by district and lower levels (hobli, grama_panchayath)
#             - "hobli_name": grouped by hobli_name and lower levels (grama_panchayath)
#             - "grama_panchayath": grouped by grama_panchayath only
#             - empty if not provided by the user
#         aggregation_value: (Optional) Value to aggregate by. If not provided, all values are returned. If provided, only the data for that value is returned.

#     Returns:
#         A dictionary with:
#         - data: Dictionary with aggregation levels as keys and their respective data as values
#             * If "point": {"point": [{"latitude": float, "longitude": float, "count": int, "location": str}, ...]}
#             * Otherwise: {"state": [...], "district": [...], "hobli_name": [...], "grama_panchayath": [...]}
#         - meta: Metadata including time range and whether defaults were applied
#         - instructions: Message for LLM to explain assumptions and guide the user
#     """

#     await insert_query(
#         "extract_data_from_samaajdata",
#         {
#             "start_date": start_date,
#             "end_date": end_date,
#             "category": category,
#             "subcategory": subcategory,
#             "type": type,
#             "aggregate_by": aggregate_by,
#             "aggregation_value": aggregation_value,
#         },
#     )

#     # Define aggregation hierarchy
#     hierarchy = ["state", "district", "hobli_name", "grama_panchayath", "point"]

#     # Default date range handling
#     if start_date:
#         start = pd.to_datetime(start_date, dayfirst=True)
#     if end_date:
#         end = pd.to_datetime(end_date, dayfirst=True)
#     if not start_date:
#         start = datetime(2000, 1, 1)
#     if not end_date:
#         end = datetime.today()

#     if not aggregate_by:
#         aggregate_by = "district"
#         default_agg_applied = True
#     else:
#         default_agg_applied = False

#     # Build base filters
#     filters = []
#     if start_date:
#         filters.append(f"e.creation >= '{start.date().isoformat()}'")
#     if end_date:
#         filters.append(f"e.creation <= '{end.date().isoformat()}'")
#     if category:
#         filters.append(f"e.category = '{category}'")
#     if subcategory:
#         filters.append(f"e.subcategory = '{subcategory}'")
#     if type:
#         filters.append(f"e.type = '{type}'")

#     base_where_clause = " AND ".join(filters) if filters else "1=1"

#     conn: asyncpg.Connection = await get_db_connection()

#     result_data = {}

#     if aggregate_by == "point":
#         # Handle point aggregation separately
#         where_clause = base_where_clause
#         if aggregation_value:
#             # For point aggregation, aggregation_value could be used to filter by location
#             where_clause += f" AND (l.state = '{aggregation_value}' OR l.district = '{aggregation_value}' OR l.hobli_name = '{aggregation_value}' OR l.grama_panchayath = '{aggregation_value}')"

#         query = f"""
#         SELECT
#             e.latitude::float AS latitude,
#             e.longitude::float AS longitude,
#             e.title AS title,
#             e.category AS category,
#             e.subcategory AS subcategory,
#             e.type AS type,
#             COUNT(*) AS count,
#             COALESCE(l.city, l.district, l.state, e.location, l.hobli_name, l.grama_panchayath) AS location
#         FROM "tabEvents" e
#         LEFT JOIN "tabLocation" l ON e.location = l.name
#         WHERE
#             e.latitude IS NOT NULL
#             AND e.longitude IS NOT NULL
#             AND e.latitude ~ '^[0-9.+-]+$'
#             AND e.longitude ~ '^[0-9.+-]+$'
#             AND {where_clause}
#         GROUP BY e.latitude, e.longitude, location, l.city, l.district, l.state, l.hobli_name, l.grama_panchayath, e.title, e.category, e.subcategory, e.type
#         ORDER BY count DESC
#         """

#         await ctx.debug(f"Point Query:\n{query}")
#         rows = await conn.fetch(query)

#         result_data["point"] = [
#             {
#                 "latitude": row["latitude"],
#                 "longitude": row["longitude"],
#                 "count": row["count"],
#                 "location": row["location"],
#                 "title": row["title"],
#                 "category": row["category"],
#                 "subcategory": row["subcategory"],
#                 "type": row["type"],
#             }
#             for row in rows
#         ]
#     else:
#         # Handle multi-level aggregation
#         # Get the index of the requested aggregation level
#         start_index = hierarchy.index(aggregate_by)

#         # Get all levels from the requested level down to grama_panchayath
#         levels_to_aggregate = hierarchy[start_index:-1]  # Exclude "point"

#         for level in levels_to_aggregate:
#             where_clause = base_where_clause
#             if aggregation_value:
#                 where_clause += f" AND l.{aggregate_by} = '{aggregation_value}'"

#             query = f"""
#             SELECT
#                 l.{level} AS location,
#                 COUNT(*) AS count
#             FROM "tabEvents" e
#             LEFT JOIN "tabLocation" l ON e.location = l.name
#             WHERE
#                 e.latitude IS NOT NULL
#                 AND e.longitude IS NOT NULL
#                 AND e.latitude ~ '^[0-9.+-]+$'
#                 AND e.longitude ~ '^[0-9.+-]+$'
#                 AND {where_clause}
#                 AND l.{level} IS NOT NULL
#                 AND l.{level} != ''
#             GROUP BY l.{level}
#             ORDER BY count DESC
#             LIMIT 100
#             """

#             await ctx.debug(f"{level.capitalize()} Query:\n{query}")
#             rows = await conn.fetch(query)

#             result_data[level] = [
#                 {"location": row["location"], "count": row["count"]}
#                 for row in rows
#                 if row["location"]
#             ]

#     await conn.close()

#     instructions = (
#         "Surface the assumptions used (like time range = past 30 days, aggregation = district) to the user if any.\n"
#         "Encourage them to try more specific queries like:\n"
#         "- 'Show me events in March by hobli'\n"
#         "- 'Cluster sanitation issues in Karnataka by grama panchayat'\n"
#         "- 'Where are fire incidents concentrated this year?'\n"
#         "If the number of issues is very high (if it will exceed your context limit) and the user wants you to do further analysis on the raw data points, ask them to use the filters to reduce the number of issues first.\n"
#         "Empty location values mean the location data wasn't available for that field."
#     )

#     output = {
#         "data": result_data,
#         "meta": {
#             "defaults_applied": {
#                 "aggregation": default_agg_applied,
#             },
#             "time_range_used": {
#                 "start": start.date().isoformat(),
#                 "end": end.date().isoformat(),
#             },
#             "aggregation_used": aggregate_by,
#             "aggregation_levels_returned": list(result_data.keys()),
#         },
#     }

#     if default_agg_applied:
#         output["instructions"] = instructions

#     return output


# @mcp.tool()
# async def get_event_trend_over_time_from_samaajdata(
#     ctx: Context,
#     start_date: Optional[str] = None,
#     end_date: Optional[str] = None,
#     category: Optional[str] = None,
#     subcategory: Optional[str] = None,
#     type: Optional[str] = None,
#     interval: Literal["month", "week", "year"] = "month",
#     filter_date_type: Optional[Literal["year", "month"]] = None,
#     filter_date_value: Optional[str] = None,
#     aggregation_level: Optional[
#         Literal["district", "state", "hobli_name", "grama_panchayath"]
#     ] = None,
#     aggregation_value: Optional[str] = None,
# ) -> dict:
#     """
#     Returns a time series of event counts to track trends over time, grouped by the given interval from Samaajdata.

#     This should be used only when the user query can be answered with the filters provided here. If the user's query requires more filters, use the get_data_metadata_on_samaajdata() tool to get the list of key-value pairs available for each event/data point and get the data field values using get_data_field_values_on_samaajdata() tool.

#     Parameters:
#         ctx: Internal MCP context (do not supply manually).
#         start_date: (Optional, format: DD/MM/YYYY) Start date for filtering event creation date.
#         end_date: (Optional, format: DD/MM/YYYY) End date for filtering event creation date.
#         category: (Optional) Filter by event category.
#                   Use values from get_valid_categories().
#         subcategory: (Optional) Filter by event subcategory.
#                      Use values from get_valid_subcategories().
#         type: (Optional) Filter by event type.
#               Use values from get_valid_event_types().
#         interval: Time grouping level. One of "month" (default), "week", or "year".
#         aggregation_level: (Optional) Geographic level to match ("district", "state", "hobli_name", or "grama_panchayath").
#         aggregation_value: (Optional) Name of the area to match (e.g., "Ludhiana" if aggregation_level="district").
#     Returns:
#         A dictionary with:
#         - trend: List of dicts with 'period' and 'count'
#         - meta: Time range used and filters applied
#         - instructions: Message for the LLM to explain results and suggest deeper analysis
#     """

#     await insert_query(
#         "get_event_trend_over_time_from_samaajdata",
#         {
#             "start_date": start_date,
#             "end_date": end_date,
#             "category": category,
#             "subcategory": subcategory,
#             "type": type,
#             "interval": interval,
#             "filter_date_type": filter_date_type,
#             "filter_date_value": filter_date_value,
#             "aggregation_level": aggregation_level,
#             "aggregation_value": aggregation_value,
#         },
#     )

#     if start_date:
#         start = pd.to_datetime(start_date, dayfirst=True)

#     if end_date:
#         end = pd.to_datetime(end_date, dayfirst=True)
#     else:
#         end = datetime.today()

#     filters = []
#     if start_date:
#         filters.append(f"e.creation >= '{start.date().isoformat()}'")
#     if end_date:
#         filters.append(f"e.creation <= '{end.date().isoformat()}'")
#     if category:
#         filters.append(f"e.category = '{category}'")
#     if subcategory:
#         filters.append(f"e.subcategory = '{subcategory}'")
#     if type:
#         filters.append(f"e.type = '{type}'")
#     if aggregation_level:
#         filters.append(f"l.{aggregation_level} = '{aggregation_value}'")
#     if filter_date_type and filter_date_value:
#         if filter_date_type == "year":
#             filters.append(f"EXTRACT(year FROM e.creation) = {filter_date_value}")
#         elif filter_date_type == "month":
#             filters.append(f"to_char(e.creation, 'YYYY-MM') = '{filter_date_value}'")

#     where_clause = " AND ".join(filters)

#     # Time truncation
#     time_field = {
#         "month": "to_char(date_trunc('month', e.creation), 'YYYY-MM')",
#         "week": "to_char(date_trunc('week', e.creation), 'IYYY-IW')",
#         "year": "to_char(date_trunc('year', e.creation), 'YYYY')",
#     }[interval]

#     await ctx.debug(f"where_clause: {where_clause}")

#     query = f"""
#     SELECT
#         {time_field} AS period,
#         COUNT(*) AS count
#     FROM tabEvents e
#     LEFT JOIN "tabLocation" l ON e.location = l.name
#     WHERE {where_clause}
#     GROUP BY period
#     ORDER BY period
#     """

#     await ctx.debug(f"Query:\n{query}")

#     conn: asyncpg.Connection = await get_db_connection()
#     rows = await conn.fetch(query)

#     trend = [{"period": row["period"], "count": row["count"]} for row in rows]

#     return {
#         "trend": trend,
#         "instructions": "Display this as a line or bar chart showing trends over time. Use the 'interval' field to choose the x-axis (month, week, year). You can ask follow-ups like: 'Break this down by location', 'Compare this with sanitation issues', 'Show me peak months of complaints'",
#     }


@mcp.tool()
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
        where_clauses.append(
            f"em.event_category IN ({', '.join([f'\'{cat}\'' for cat in event_categories])})"
        )
    if event_subcategories:
        where_clauses.append(
            f"em.event_subcategory IN ({', '.join([f'\'{subcat}\'' for subcat in event_subcategories])})"
        )
    if partners:
        where_clauses.append(
            f"em.partner IN ({', '.join([f'\'{partner}\'' for partner in partners])})"
        )

    where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

    query = f"""
        SELECT COUNT(DISTINCT em.event_id) as count
        FROM "Events Metadata" em
        WHERE {where_clause}
    """

    await ctx.debug(f"Query: {query}")

    count_row = await conn.fetchrow(query)
    await conn.close()
    return {"count": count_row["count"] if count_row else 0}


@mcp.tool()
async def get_data_metadata_on_samaajdata(
    ctx: Context,
    event_categories: list[str],
    event_subcategories: list[str],
    partners: list[str],
) -> dict:
    """
    Returns the list of fields for all the data matching the given filters on SamaajData.
    When any information is asked about a specific data source or data set or partner data, use this tool to get the list of fields/data points that are available about that data in the database and then, based on the field names and field descriptions, decide if the user's query can be answered with the data available.
    Get the list of partners along with the appropriate event categories and subcategories using get_data_partners_list() tool.
    """

    conn: asyncpg.Connection = await get_db_connection()
    where_clauses = []
    if event_categories:
        where_clauses.append(
            f"e.event_category IN ({', '.join([f'\'{cat}\'' for cat in event_categories])})"
        )
    if event_subcategories:
        where_clauses.append(
            f"e.event_subcategory IN ({', '.join([f'\'{subcat}\'' for subcat in event_subcategories])})"
        )
    if partners:
        where_clauses.append(
            f"e.partner IN ({', '.join([f'\'{partner}\'' for partner in partners])})"
        )

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

    field_rows = await conn.fetch(fields_query)

    # Then, get example values for each field
    rows = []
    for field_row in field_rows:
        field_name = field_row["field_name"]

        examples_query = f"""
            SELECT DISTINCT field_value
            FROM "Events Metadata"
            WHERE field_name = $1
              AND event_category IN ({', '.join([f'\'{cat}\'' for cat in event_categories])})
              AND event_subcategory IN ({', '.join([f'\'{subcat}\'' for subcat in event_subcategories])})
              AND partner IN ({', '.join([f'\'{partner}\'' for partner in partners])})
              AND location_id IS NOT NULL
              AND field_value IS NOT NULL
            LIMIT 10
        """

        await ctx.debug(f"Examples Query for {field_name}: {examples_query}")

        example_rows = await conn.fetch(examples_query, field_name)
        example_values = [row["field_value"] for row in example_rows]

        rows.append(
            {
                "field_name": field_row["field_name"],
                "field_definition": field_row["field_definition"],
                "example_values": example_values,
            }
        )

    await conn.close()

    return {
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


@mcp.tool()
async def get_data_field_values_on_samaajdata(
    ctx: Context,
    field_name: str,
    event_categories: list[str],
    event_subcategories: list[str],
    partners: list[str],
    aggregation_type: Optional[Literal["unique", "count"]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
    segregate_by_location: Optional[Literal["city", "state"]] = None,
    segregate_by_time: Optional[Literal["day", "month", "year"]] = None,
) -> dict:
    """
    Returns all values for a given field from data matching the given filters on SamaajData,
    with optional segregation by location and/or time.

    Parameters:
        ctx: Internal MCP context (do not supply manually).
        field_name: (Required) The name of the field to get values for.
        event_categories: Filter by event category.
                  Use values from get_valid_categories().
        event_subcategories: Filter by event subcategory.
                     Use values from get_valid_subcategories().
        partners: Filter by partner.
                  Use values from get_data_partners_list().
        aggregation_type: (Optional, Literal["unique", "count"]) The type of aggregation to perform on the field values.
        start_date: (Optional, format: DD/MM/YYYY) Start date for filtering event creation.
        end_date: (Optional, format: DD/MM/YYYY) End date for filtering event creation.
        city: (Optional) Filter by city.
        state: (Optional) Filter by state.
        segregate_by_location: (Optional, Literal["city", "state"]) Segregate results by city or state.
        segregate_by_time: (Optional, Literal["day", "month", "year"]) Segregate results by day, month, or year.

    Returns:
        dict: Structure varies based on segregation options:
        - No segregation: {"values": [value_1, value_2, ...]} or {"values": {"value_1": count_1, ...}}
        - Location only: {"values": {"location_1": [...], "location_2": [...]}}
        - Time only: {"values": {"time_period_1": [...], "time_period_2": [...]}}
        - Both: {"values": {"location_1": {"time_period_1": [...], ...}, ...}}
    """

    conn: asyncpg.Connection = await get_db_connection()

    filters = ["em.location_id IS NOT NULL", f"em.field_name = '{field_name}'"]
    params = {}

    if start_date:
        try:
            start_dt = datetime.strptime(start_date, "%d/%m/%Y")
            filters.append(f"em.creation >= '{start_dt.date().isoformat()}'")
        except Exception:
            pass
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, "%d/%m/%Y")
            filters.append(f"em.creation <= '{end_dt.date().isoformat()}'")
        except Exception:
            pass
    if city:
        filters.append(f"em.city = '{city}'")
    if state:
        filters.append(f"em.state = '{state}'")

    # Handle parametrized queries for IN clauses
    if event_categories:
        filters.append(
            f"em.event_category IN ({', '.join([f'\'{cat}\'' for cat in event_categories])})"
        )
    if event_subcategories:
        filters.append(
            f"em.event_subcategory IN ({', '.join([f'\'{subcat}\'' for subcat in event_subcategories])})"
        )
    if partners:
        filters.append(
            f"em.partner IN ({', '.join([f'\'{partner}\'' for partner in partners])})"
        )

    where_clause = " AND ".join(filters)

    # Build SELECT clause based on segregation options
    select_fields = ["em.field_value"]
    if segregate_by_location:
        select_fields.append(f"em.{segregate_by_location}")
    if segregate_by_time:
        if segregate_by_time == "day":
            select_fields.append("DATE(em.creation) as time_period")
        elif segregate_by_time == "month":
            select_fields.append("TO_CHAR(em.creation, 'YYYY-MM') as time_period")
        elif segregate_by_time == "year":
            select_fields.append("EXTRACT(YEAR FROM em.creation) as time_period")

    query = f"""
        SELECT {', '.join(select_fields)}
        FROM "Events Metadata" em
        WHERE {where_clause}
    """

    await ctx.debug(f"Query: {query}")

    rows = await conn.fetch(query)

    await conn.close()

    # Process results based on segregation options
    if not segregate_by_location and not segregate_by_time:
        # No segregation - original behavior
        results = []
        for row in rows:
            field_value = row["field_value"]
            if field_value and field_value.strip():
                results.append(field_value)

        if aggregation_type == "unique":
            results = list(set(results))
        elif aggregation_type == "count":
            results = dict(Counter(results))

        if isinstance(results, list) and len(results) > 1000:
            results = random.sample(results, 1000)

        return {"values": results}

    else:
        # Segregated results
        segregated_data = {}

        for row in rows:
            field_value = row["field_value"]
            if not field_value or not field_value.strip():
                continue

            # Determine primary key (location or time or both)
            keys = []
            if segregate_by_location:
                location_key = row[segregate_by_location] or "Unknown"
                keys.append(str(location_key))
            if segregate_by_time:
                time_key = row["time_period"] or "Unknown"
                keys.append(str(time_key))

            # Build nested structure
            current_dict = segregated_data
            for i, key in enumerate(keys[:-1]):
                if key not in current_dict:
                    current_dict[key] = {}
                current_dict = current_dict[key]

            # Add to the final level
            final_key = keys[-1]
            if final_key not in current_dict:
                current_dict[final_key] = []
            current_dict[final_key].append(field_value)

        # Apply aggregation to leaf nodes
        def apply_aggregation(data):
            if isinstance(data, list):
                if aggregation_type == "unique":
                    result = list(set(data))
                    return result[:1000] if len(result) > 1000 else result
                elif aggregation_type == "count":
                    return dict(Counter(data))
                else:
                    return data[:1000] if len(data) > 1000 else data
            elif isinstance(data, dict):
                return {k: apply_aggregation(v) for k, v in data.items()}
            return data

        segregated_data = apply_aggregation(segregated_data)
        return {"values": segregated_data}


@mcp.tool()
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

    Parameters:
        ctx: Internal MCP context (do not supply manually).
        data: List of string values that will be counted to create the pie chart (e.g., ["Category A", "Category A", "Category B", "Category B", "Category B"]).
        title: (Optional) Title for the pie chart.
        colors: (Optional) List of color names/hex codes for the pie slices. If not provided, uses default matplotlib colors.
        width: (Optional) Width of the figure in inches. Default is 10.
        height: (Optional) Height of the figure in inches. Default is 8.
        show_percentages: (Optional) Whether to show percentages on the pie slices. Default is True.
        start_angle: (Optional) Starting angle for the first slice in degrees. Default is 90 (top of circle).

    Returns:
        Dictionary with:
        - public_url: Public URL of the image
        - data_summary: Summary of the input data including counts
        - title: Title of the pie chart
        - instructions: Instructions for the user
    """

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

        # Count occurrences of each unique value
        value_counts = Counter(filtered_data)

        # Extract labels and values
        labels = list(value_counts.keys())
        values = list(value_counts.values())

        # Create figure
        fig, ax = plt.subplots(figsize=(width, height))

        # Calculate percentages for legend
        total = sum(values)
        percentages = [(value / total * 100) for value in values]

        # Create labels with percentages for legend
        legend_labels = []
        for i, (label, pct) in enumerate(zip(labels, percentages)):
            if show_percentages:
                legend_labels.append(f"{label} ({pct:.1f}%)")
            else:
                legend_labels.append(label)

        # For small slices (less than 5%), don't show percentage on the slice itself
        # For larger slices, show percentage on the slice
        def autopct_func(pct):
            if pct < 5.0:  # Small slices - don't show percentage on slice
                return ""
            return f"{pct:.1f}%"

        autopct = autopct_func if show_percentages else None

        # Create pie chart without labels (we'll use legend instead)
        if show_percentages:
            wedges, texts, autotexts = ax.pie(
                values,
                labels=None,  # Remove labels from pie chart
                autopct=autopct,
                startangle=start_angle,
                colors=colors,
                pctdistance=0.85,  # Move percentages closer to edge for better readability
            )
        else:
            wedges, texts = ax.pie(
                values,
                labels=None,  # Remove labels from pie chart
                autopct=autopct,
                startangle=start_angle,
                colors=colors,
            )
            autotexts = None

        # Add legend outside the pie chart
        ax.legend(
            wedges,
            legend_labels,
            title="Categories",
            loc="center left",
            bbox_to_anchor=(1, 0, 0.5, 1),
            fontsize=10,
        )

        # Set title if provided
        if title:
            ax.set_title(title, fontsize=16, fontweight="bold", pad=20)

        # Ensure equal aspect ratio for circular pie
        ax.axis("equal")

        # Improve text readability for percentages shown on slices
        if show_percentages and autotexts:
            for autotext in autotexts:
                autotext.set_color("white")
                autotext.set_fontweight("bold")
                autotext.set_fontsize(9)

        # Adjust layout to accommodate legend
        plt.subplots_adjust(left=0.1, right=0.75)  # Make room for legend on the right
        plt.tight_layout()

        # Save to bytes buffer
        img_buffer = io.BytesIO()
        plt.savefig(
            img_buffer,
            format="png",
            dpi=300,
            bbox_inches="tight",
            facecolor="white",
            edgecolor="none",
        )

        # Close the figure to free memory
        plt.close(fig)

        # Prepare data summary
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
        upload_result = upload_image_to_s3(img_buffer)

        return {
            "public_url": upload_result["public_url"],
            "data_summary": data_summary,
            "title": title or "Pie Chart",
            "instructions": f"The pie chart has been uploaded to S3 at {upload_result['public_url']}. The data_summary provides details about the chart contents including counts and percentages.",
        }

    except Exception as e:
        await ctx.debug(f"Error creating pie chart: {str(e)}")
        return {
            "error": f"Failed to create pie chart: {str(e)}",
            "s3_bucket": None,
            "s3_key": None,
            "public_url": None,
            "chart_type": "pie_chart",
        }


@mcp.tool()
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

    **When to use**: Comparing categories, showing counts/values across different groups,
    comparing performance metrics, displaying survey results, or any categorical data comparison.

    **Variants supported**:
    - Grouped: Multiple series side by side (default)
    - Stacked: Series stacked on top of each other
    - Horizontal: Horizontal bars instead of vertical columns

    Parameters:
        ctx: Internal MCP context (do not supply manually).
        data: Dictionary containing the chart data. Format options:
            Simple bar chart: {"categories": ["A", "B", "C"], "values": [10, 20, 15]}
            Grouped/Stacked: {"categories": ["A", "B", "C"], "Series1": [10, 20, 15], "Series2": [5, 15, 25]}
            From raw data: {"raw_data": ["A", "A", "B", "B", "B", "C"], "group_by": None}
            Grouped raw data: {"raw_data": [{"category": "A", "group": "X", "value": 10}, ...]}
        title: (Optional) Title for the bar chart.
        x_label: (Optional) Label for the x-axis.
        y_label: (Optional) Label for the y-axis.
        colors: (Optional) List of color names/hex codes for the bars/series.
        width: (Optional) Width of the figure in inches. Default is 12.
        height: (Optional) Height of the figure in inches. Default is 8.
        chart_style: (Optional) Style of bar chart: "grouped", "stacked", or "horizontal". Default is "grouped".
        show_values: (Optional) Whether to show values on top of bars. Default is True.
        rotation: (Optional) Rotation angle for x-axis labels in degrees. Default is 0.

    Returns:
        Dictionary with public URL of the image, data summary, title, and instructions.

    **Example usage**:
    - Simple: {"categories": ["Q1", "Q2", "Q3", "Q4"], "values": [100, 150, 120, 180]}
    - Multi-series: {"categories": ["A", "B", "C"], "Male": [20, 30, 25], "Female": [15, 35, 20]}
    - From counts: {"raw_data": ["Apple", "Apple", "Orange", "Apple", "Orange"]}
    """

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
        if not data or not isinstance(data, dict):
            raise ValueError("Data must be a non-empty dictionary")

        # Process different data formats
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

        # Create figure
        fig, ax = plt.subplots(figsize=(width, height))

        # Set up colors
        if colors is None:
            colors = plt.cm.Set3(range(len(series_data)))
        elif len(colors) < len(series_data):
            # Extend colors if not enough provided
            default_colors = plt.cm.Set3(range(len(series_data)))
            colors.extend(default_colors[len(colors) :])

        # Create bars based on style
        if chart_style == "horizontal":
            # Horizontal bar chart
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
            # Stacked bar chart
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
            # Grouped bar chart
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

        # Set title
        if title:
            ax.set_title(title, fontsize=16, fontweight="bold", pad=20)

        # Improve layout
        plt.tight_layout()

        # Save to bytes buffer
        img_buffer = io.BytesIO()
        plt.savefig(
            img_buffer,
            format="png",
            dpi=300,
            bbox_inches="tight",
            facecolor="white",
            edgecolor="none",
        )

        # Close the figure to free memory
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
        upload_result = upload_image_to_s3(img_buffer)

        return {
            "public_url": upload_result["public_url"],
            "data_summary": data_summary,
            "title": title or "Bar Chart",
            "instructions": f"The bar chart has been uploaded to S3 at {upload_result['public_url']}. The data_summary provides details about the chart configuration and data.",
        }

    except Exception as e:
        await ctx.debug(f"Error creating bar chart: {str(e)}")
        return {
            "error": f"Failed to create bar chart: {str(e)}",
            "s3_bucket": None,
            "s3_key": None,
            "public_url": None,
        }


@mcp.tool()
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
    """
    Creates a line plot image and returns the public URL of the image.

    **When to use**: Showing trends over time, ordered data progression, time series analysis,
    continuous data relationships, or any sequential data where the order matters.

    **Variants supported**:
    - Multi-series: Multiple lines on the same plot
    - Smoothed lines: Apply spline interpolation for smoother curves
    - Area fill: Fill area under lines for emphasis
    - Various line styles and markers

    Parameters:
        ctx: Internal MCP context (do not supply manually).
        data: Dictionary containing the line plot data. Format options:
            Simple line: {"x": [1, 2, 3, 4], "y": [10, 20, 15, 25]}
            Multi-series: {"x": [1, 2, 3, 4], "Series1": [10, 20, 15, 25], "Series2": [5, 15, 25, 20]}
            Time series: {"dates": ["2023-01", "2023-02", "2023-03"], "values": [100, 150, 120]}
            From raw data: {"raw_data": [{"x": 1, "y": 10}, {"x": 2, "y": 20}, ...]}
        title: (Optional) Title for the line plot.
        x_label: (Optional) Label for the x-axis.
        y_label: (Optional) Label for the y-axis.
        colors: (Optional) List of color names/hex codes for the lines.
        width: (Optional) Width of the figure in inches. Default is 12.
        height: (Optional) Height of the figure in inches. Default is 8.
        line_style: (Optional) Style of lines: "solid", "dashed", "dotted", "dashdot". Default is "solid".
        marker_style: (Optional) Marker style for data points (e.g., 'o', 's', '^', 'D'). Default is "o".
        show_markers: (Optional) Whether to show markers on data points. Default is True.
        show_grid: (Optional) Whether to show grid lines. Default is True.
        smooth_lines: (Optional) Whether to apply spline smoothing to lines. Default is False.
        fill_area: (Optional) Whether to fill area under lines. Default is False.

    Returns:
        Dictionary with public URL of the image, data summary, title, and instructions.

    **Example usage**:
    - Time series: {"dates": ["Jan", "Feb", "Mar"], "Revenue": [1000, 1200, 1100]}
    - Multi-series: {"months": [1,2,3,4], "Product A": [10,15,12,18], "Product B": [8,12,16,14]}
    - Smooth trend: {"x": [1,2,3,4,5], "y": [10,25,20,35,30]} with smooth_lines=True
    """

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
        # Validate input data
        if not data or not isinstance(data, dict):
            raise ValueError("Data must be a non-empty dictionary")

        # Process different data formats
        x_data = None
        series_data = {}

        if "raw_data" in data:
            # Handle raw data format
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
            # Handle structured data
            # Find x-axis data
            x_keys = ["x", "dates", "time", "months", "years", "categories"]
            for key in x_keys:
                if key in data:
                    x_data = data[key]
                    break

            if x_data is None:
                raise ValueError(
                    "Data must contain x-axis data (x, dates, time, months, years, or categories)"
                )

            # Find y-axis series
            for key, values in data.items():
                if key not in x_keys and isinstance(values, list):
                    if len(values) != len(x_data):
                        raise ValueError(
                            f"Series '{key}' length ({len(values)}) doesn't match x-axis length ({len(x_data)})"
                        )
                    series_data[key] = values

        if not x_data or not series_data:
            raise ValueError("No valid data found for plotting")

        # Create figure
        fig, ax = plt.subplots(figsize=(width, height))

        # Set up colors
        if colors is None:
            colors = plt.cm.tab10(range(len(series_data)))
        elif len(colors) < len(series_data):
            # Extend colors if not enough provided
            default_colors = plt.cm.tab10(range(len(series_data)))
            colors.extend(default_colors[len(colors) :])

        # Convert line style string to matplotlib format
        line_styles = {"solid": "-", "dashed": "--", "dotted": ":", "dashdot": "-."}
        ls = line_styles.get(line_style, "-")

        # Plot each series
        for i, (series_name, y_values) in enumerate(series_data.items()):
            # Prepare data for plotting
            x_plot = list(range(len(x_data))) if isinstance(x_data[0], str) else x_data
            y_plot = y_values

            # Apply smoothing if requested and scipy is available
            if smooth_lines and SCIPY_AVAILABLE and len(x_plot) > 3:
                try:
                    # Create smooth curve using spline interpolation
                    x_smooth = np.linspace(min(x_plot), max(x_plot), len(x_plot) * 3)
                    x_array = np.array(x_plot)
                    y_array = np.array(y_plot)

                    # Sort by x values for spline interpolation
                    sort_idx = np.argsort(x_array)
                    x_sorted = x_array[sort_idx]
                    y_sorted = y_array[sort_idx]

                    spl = make_interp_spline(
                        x_sorted, y_sorted, k=min(3, len(x_sorted) - 1)
                    )
                    y_smooth = spl(x_smooth)

                    # Plot smooth line
                    ax.plot(
                        x_smooth,
                        y_smooth,
                        linestyle=ls,
                        color=colors[i],
                        label=series_name,
                        linewidth=2,
                        alpha=0.8,
                    )

                    # Plot original points as markers if requested
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
                    # Fall back to regular line if smoothing fails
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
                # Regular line plot
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

            # Fill area under curve if requested
            if fill_area:
                ax.fill_between(x_plot, y_plot, alpha=0.3, color=colors[i])

        # Set x-axis labels
        if isinstance(x_data[0], str):
            ax.set_xticks(range(len(x_data)))
            ax.set_xticklabels(
                x_data, rotation=45 if len(max(x_data, key=len)) > 8 else 0
            )

        # Add legend if multiple series
        if len(series_data) > 1:
            ax.legend()

        # Set labels and title
        if x_label:
            ax.set_xlabel(x_label)
        if y_label:
            ax.set_ylabel(y_label)
        if title:
            ax.set_title(title, fontsize=16, fontweight="bold", pad=20)

        # Add grid if requested
        if show_grid:
            ax.grid(True, alpha=0.3)

        # Improve layout
        plt.tight_layout()

        # Save to bytes buffer
        img_buffer = io.BytesIO()
        plt.savefig(
            img_buffer,
            format="png",
            dpi=300,
            bbox_inches="tight",
            facecolor="white",
            edgecolor="none",
        )

        # Close the figure to free memory
        plt.close(fig)

        # Prepare data summary
        data_summary = {
            "series_count": len(series_data),
            "series_names": list(series_data.keys()),
            "data_points_per_series": len(x_data),
            "x_axis_type": "categorical" if isinstance(x_data[0], str) else "numerical",
            "smoothing_applied": smooth_lines and SCIPY_AVAILABLE,
            "area_filled": fill_area,
        }

        # Upload to S3
        upload_result = upload_image_to_s3(img_buffer)

        return {
            "public_url": upload_result["public_url"],
            "data_summary": data_summary,
            "title": title or "Line Plot",
            "instructions": f"The line plot has been uploaded to S3 at {upload_result['public_url']}. The data_summary provides details about the chart configuration and data.",
        }

    except Exception as e:
        await ctx.debug(f"Error creating line plot: {str(e)}")
        return {
            "error": f"Failed to create line plot: {str(e)}",
            "s3_bucket": None,
            "s3_key": None,
            "public_url": None,
        }


@mcp.tool()
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
    """
    Creates a histogram image and returns the public URL of the image.

    **When to use**: Analyzing the distribution of a single variable, understanding data spread,
    identifying patterns like skewness, outliers, multiple modes, or normal distribution.
    Essential for statistical analysis and data exploration.

    **Use cases**:
    - Age distribution in a population
    - Income ranges analysis
    - Test scores distribution
    - Response times distribution
    - Any continuous variable frequency analysis

    Parameters:
        ctx: Internal MCP context (do not supply manually).
        data: List of numerical values to create the histogram from.
        title: (Optional) Title for the histogram.
        x_label: (Optional) Label for the x-axis (variable being measured).
        y_label: (Optional) Label for the y-axis (frequency or density).
        bins: (Optional) Number of bins for the histogram. Default is 30.
        color: (Optional) Color name/hex code for the bars. Default is "skyblue".
        width: (Optional) Width of the figure in inches. Default is 10.
        height: (Optional) Height of the figure in inches. Default is 8.
        show_density: (Optional) Whether to show density instead of frequency. Default is False.
        show_stats: (Optional) Whether to show basic statistics on the plot. Default is True.
        alpha: (Optional) Transparency of the bars (0-1). Default is 0.7.

    Returns:
        Dictionary with public URL of the image, data summary, title, and instructions.

    **Example usage**:
    - Age distribution: [25, 30, 35, 28, 42, 38, 29, 31, 45, 27, ...]
    - Test scores: [85, 92, 78, 88, 95, 82, 79, 91, 87, 84, ...]
    - Response times: [0.5, 1.2, 0.8, 2.1, 0.9, 1.5, 0.7, 1.8, ...]
    """

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
        # Validate input data
        if not data:
            raise ValueError("Data cannot be empty")

        if not isinstance(data, list):
            raise ValueError("Data must be a list of numerical values")

        # Filter out None values and convert to float
        filtered_data = []
        for item in data:
            if item is not None:
                try:
                    filtered_data.append(float(item))
                except (ValueError, TypeError):
                    pass  # Skip non-numeric values

        if not filtered_data:
            raise ValueError("No valid numerical data found")

        # Convert to numpy array for statistical calculations
        data_array = np.array(filtered_data)

        # Calculate statistics
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

        # Create figure
        fig, ax = plt.subplots(figsize=(width, height))

        # Create histogram
        n, bins_edges, patches = ax.hist(
            filtered_data,
            bins=bins,
            color=color,
            alpha=alpha,
            density=show_density,
            edgecolor="black",
            linewidth=0.5,
        )

        # Set labels and title
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

        # Add statistics text box if requested
        if show_stats:
            stats_text = f"""Statistics:
Count: {stats['count']:,}
Mean: {stats['mean']:.2f}
Median: {stats['median']:.2f}
Std Dev: {stats['std']:.2f}
Range: {stats['min']:.2f} - {stats['max']:.2f}"""

            # Position text box in upper right
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

        # Add mean line
        ax.axvline(
            stats["mean"],
            color="red",
            linestyle="--",
            linewidth=2,
            label=f'Mean: {stats["mean"]:.2f}',
            alpha=0.8,
        )

        # Add median line
        ax.axvline(
            stats["median"],
            color="green",
            linestyle="--",
            linewidth=2,
            label=f'Median: {stats["median"]:.2f}',
            alpha=0.8,
        )

        # Add legend for the lines
        ax.legend()

        # Add grid for better readability
        ax.grid(True, alpha=0.3)

        # Improve layout
        plt.tight_layout()

        # Save to bytes buffer
        img_buffer = io.BytesIO()
        plt.savefig(
            img_buffer,
            format="png",
            dpi=300,
            bbox_inches="tight",
            facecolor="white",
            edgecolor="none",
        )

        # Close the figure to free memory
        plt.close(fig)

        # Prepare data summary
        data_summary = {
            "statistics": stats,
            "bins_used": len(bins_edges) - 1,
            "density_mode": show_density,
            "data_range": stats["max"] - stats["min"],
            "bin_width": (stats["max"] - stats["min"]) / bins if bins > 0 else 0,
        }

        # Upload to S3
        upload_result = upload_image_to_s3(img_buffer)

        return {
            "public_url": upload_result["public_url"],
            "data_summary": data_summary,
            "title": title or "Histogram",
            "instructions": f"The histogram has been uploaded to S3 at {upload_result['public_url']}. The data_summary includes detailed statistics about the distribution.",
        }

    except Exception as e:
        await ctx.debug(f"Error creating histogram: {str(e)}")
        return {
            "error": f"Failed to create histogram: {str(e)}",
            "s3_bucket": None,
            "s3_key": None,
            "public_url": None,
        }


@mcp.tool()
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
    """
    Creates a box plot image and returns the public URL of the image.

    **When to use**: Comparing distributions across categories, identifying outliers,
    showing median, quartiles, and spread. Essential for statistical analysis and
    comparing multiple groups or categories.

    **Use cases**:
    - Compare test scores across different classes
    - Analyze salary distributions by department
    - Compare response times across different systems
    - Any scenario where you need to compare distributions

    Parameters:
        ctx: Internal MCP context (do not supply manually).
        data: Dictionary containing the box plot data. Format options:
            Multiple groups: {"Group A": [1, 2, 3, 4, 5], "Group B": [2, 3, 4, 5, 6]}
            Single group: {"values": [1, 2, 3, 4, 5, 6, 7, 8, 9]}
            From raw data: {"raw_data": [{"category": "A", "value": 10}, ...]}
        title: (Optional) Title for the box plot.
        x_label: (Optional) Label for the x-axis.
        y_label: (Optional) Label for the y-axis.
        colors: (Optional) List of color names/hex codes for the boxes.
        width: (Optional) Width of the figure in inches. Default is 10.
        height: (Optional) Height of the figure in inches. Default is 8.
        show_outliers: (Optional) Whether to show outlier points. Default is True.
        show_means: (Optional) Whether to show mean markers. Default is True.
        orientation: (Optional) "vertical" or "horizontal" box orientation. Default is "vertical".

    Returns:
        Dictionary with public URL of the image, data summary, title, and instructions.

    **Example usage**:
    - Multiple groups: {"Math": [85, 90, 78, 92, 88], "Science": [82, 87, 79, 91, 85]}
    - Age by gender: {"Male": [25, 30, 35, 28], "Female": [27, 32, 29, 31]}
    """

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
        # Validate input data
        if not data or not isinstance(data, dict):
            raise ValueError("Data must be a non-empty dictionary")

        # Process different data formats
        plot_data = []
        labels = []

        if "raw_data" in data:
            # Handle raw data format
            if isinstance(data["raw_data"], list) and data["raw_data"]:
                if isinstance(data["raw_data"][0], dict):
                    df = pd.DataFrame(data["raw_data"])
                    if "category" not in df.columns or "value" not in df.columns:
                        raise ValueError(
                            "Raw data must have 'category' and 'value' columns"
                        )

                    # Group by category
                    for category in df["category"].unique():
                        category_data = df[df["category"] == category]["value"].tolist()
                        # Convert to numeric, filter out non-numeric values
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
            # Handle structured data
            for key, values in data.items():
                if isinstance(values, list) and values:
                    # Convert to numeric, filter out non-numeric values
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

        # Create figure
        fig, ax = plt.subplots(figsize=(width, height))

        # Create box plot
        box_plot = ax.boxplot(
            plot_data,
            labels=labels,
            patch_artist=True,
            showmeans=show_means,
            showfliers=show_outliers,
            vert=(orientation == "vertical"),
        )

        # Set colors
        if colors is None:
            colors = plt.cm.Set3(range(len(plot_data)))
        elif len(colors) < len(plot_data):
            # Extend colors if not enough provided
            default_colors = plt.cm.Set3(range(len(plot_data)))
            colors.extend(default_colors[len(colors) :])

        # Apply colors to boxes
        for patch, color in zip(box_plot["boxes"], colors):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)

        # Customize appearance
        for element in ["whiskers", "fliers", "medians", "caps"]:
            plt.setp(box_plot[element], color="black")

        if show_means:
            plt.setp(
                box_plot["means"],
                marker="D",
                markerfacecolor="red",
                markeredgecolor="red",
            )

        # Set labels and title
        if orientation == "vertical":
            if x_label:
                ax.set_xlabel(x_label)
            if y_label:
                ax.set_ylabel(y_label)
        else:
            if x_label:
                ax.set_ylabel(x_label)  # Swapped for horizontal
            if y_label:
                ax.set_xlabel(y_label)

        if title:
            ax.set_title(title, fontsize=16, fontweight="bold", pad=20)

        # Add grid for better readability
        ax.grid(True, alpha=0.3)

        # Improve layout
        plt.tight_layout()

        # Save to bytes buffer
        img_buffer = io.BytesIO()
        plt.savefig(
            img_buffer,
            format="png",
            dpi=300,
            bbox_inches="tight",
            facecolor="white",
            edgecolor="none",
        )

        # Close the figure to free memory
        plt.close(fig)

        # Calculate summary statistics for each group
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

        # Prepare data summary
        data_summary = {
            "groups_count": len(labels),
            "group_names": labels,
            "orientation": orientation,
            "statistics": group_stats,
            "outliers_shown": show_outliers,
            "means_shown": show_means,
        }

        # Upload to S3
        upload_result = upload_image_to_s3(img_buffer)

        return {
            "public_url": upload_result["public_url"],
            "data_summary": data_summary,
            "title": title or "Box Plot",
            "instructions": f"The box plot has been uploaded to S3 at {upload_result['public_url']}. The data_summary includes statistics for each group.",
        }

    except Exception as e:
        await ctx.debug(f"Error creating box plot: {str(e)}")
        return {
            "error": f"Failed to create box plot: {str(e)}",
            "s3_bucket": None,
            "s3_key": None,
            "public_url": None,
        }


@mcp.tool()
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
    """
    Creates a scatter plot image and returns the public URL of the image.

    **When to use**: Exploring relationships between two variables, identifying correlations,
    spotting patterns, clusters, or outliers in bivariate data.

    **Use cases**:
    - Height vs Weight relationships
    - Ad spend vs Sales correlation
    - Temperature vs Ice cream sales
    - Any two continuous variables analysis

    Parameters:
        ctx: Internal MCP context (do not supply manually).
        data: Dictionary containing the scatter plot data. Format options:
            Simple: {"x": [1, 2, 3, 4], "y": [2, 4, 1, 5]}
            Multiple series: {"x": [1, 2, 3], "y": [2, 4, 1], "group": ["A", "A", "B"]}
            With size: {"x": [1, 2, 3], "y": [2, 4, 1], "size": [10, 20, 30]}
            From raw data: {"raw_data": [{"x": 1, "y": 2, "group": "A"}, ...]}
        title: (Optional) Title for the scatter plot.
        x_label: (Optional) Label for the x-axis.
        y_label: (Optional) Label for the y-axis.
        colors: (Optional) List of color names/hex codes for different groups.
        width: (Optional) Width of the figure in inches. Default is 10.
        height: (Optional) Height of the figure in inches. Default is 8.
        marker_size: (Optional) Size of the markers. Default is 50.
        marker_style: (Optional) Style of markers ('o', 's', '^', 'D', etc.). Default is 'o'.
        show_regression: (Optional) Whether to show regression line. Default is False.
        show_correlation: (Optional) Whether to show correlation coefficient. Default is True.
        alpha: (Optional) Transparency of markers (0-1). Default is 0.7.

    Returns:
        Dictionary with public URL of the image, data summary, title, and instructions.

    **Example usage**:
    - Simple: {"x": [1, 2, 3, 4], "y": [2, 4, 1, 5]}
    - Grouped: {"x": [1, 2, 3, 4], "y": [2, 4, 1, 5], "group": ["A", "A", "B", "B"]}
    - With regression: show_regression=True
    """

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
        # Validate input data
        if not data or not isinstance(data, dict):
            raise ValueError("Data must be a non-empty dictionary")

        # Process different data formats
        x_data = []
        y_data = []
        group_data = []
        size_data = []

        if "raw_data" in data:
            # Handle raw data format
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
            # Handle structured data
            if "x" not in data or "y" not in data:
                raise ValueError("Data must contain 'x' and 'y' keys")

            x_data = data["x"]
            y_data = data["y"]

            if "group" in data:
                group_data = data["group"]

            if "size" in data:
                size_data = data["size"]

        # Validate data lengths
        if len(x_data) != len(y_data):
            raise ValueError("x and y data must have the same length")

        if group_data and len(group_data) != len(x_data):
            raise ValueError("group data must have the same length as x and y")

        if size_data and len(size_data) != len(x_data):
            raise ValueError("size data must have the same length as x and y")

        # Convert to numeric
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
                pass  # Skip non-numeric values

        if not x_numeric:
            raise ValueError("No valid numerical data found")

        # Filter other data based on valid indices
        if group_data:
            group_data = [group_data[i] for i in valid_indices]
        if size_data:
            size_data = [
                float(size_data[i]) for i in valid_indices if size_data[i] is not None
            ]

        # Create figure
        fig, ax = plt.subplots(figsize=(width, height))

        # Create scatter plot
        if group_data:
            # Multiple groups
            unique_groups = list(set(group_data))

            # Set up colors
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
            # Single group
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

        # Add regression line if requested
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
                pass  # Skip regression if it fails

        # Calculate and show correlation if requested
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

        # Set labels and title
        if x_label:
            ax.set_xlabel(x_label)
        if y_label:
            ax.set_ylabel(y_label)
        if title:
            ax.set_title(title, fontsize=16, fontweight="bold", pad=20)

        # Add grid for better readability
        ax.grid(True, alpha=0.3)

        # Improve layout
        plt.tight_layout()

        # Save to bytes buffer
        img_buffer = io.BytesIO()
        plt.savefig(
            img_buffer,
            format="png",
            dpi=300,
            bbox_inches="tight",
            facecolor="white",
            edgecolor="none",
        )

        # Close the figure to free memory
        plt.close(fig)

        # Prepare data summary
        data_summary = {
            "data_points": len(x_numeric),
            "groups": len(set(group_data)) if group_data else 1,
            "correlation": correlation,
            "x_range": [min(x_numeric), max(x_numeric)],
            "y_range": [min(y_numeric), max(y_numeric)],
            "regression_shown": show_regression and SCIPY_AVAILABLE,
        }

        # Upload to S3
        upload_result = upload_image_to_s3(img_buffer)

        return {
            "public_url": upload_result["public_url"],
            "data_summary": data_summary,
            "title": title or "Scatter Plot",
            "instructions": f"The scatter plot has been uploaded to S3 at {upload_result['public_url']}. The data_summary includes correlation and range information.",
        }

    except Exception as e:
        await ctx.debug(f"Error creating scatter plot: {str(e)}")
        return {
            "error": f"Failed to create scatter plot: {str(e)}",
            "s3_bucket": None,
            "s3_key": None,
            "public_url": None,
        }


@mcp.tool()
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
    """
    Creates a donut chart image and returns the public URL of the image.

    **When to use**: Same as pie charts but with a more modern appearance. The center hole
    can be used to display key information like totals or titles. Better for displaying
    proportions with additional context in the center.

    **Use cases**:
    - Budget allocation with total budget in center
    - Market share analysis with total market size
    - Survey responses with total respondents
    - Any proportion analysis where center space adds value

    Parameters:
        ctx: Internal MCP context (do not supply manually).
        data: List of string values that will be counted to create the donut chart.
        title: (Optional) Title for the donut chart.
        colors: (Optional) List of color names/hex codes for the slices.
        width: (Optional) Width of the figure in inches. Default is 10.
        height: (Optional) Height of the figure in inches. Default is 8.
        show_percentages: (Optional) Whether to show percentages on slices. Default is True.
        start_angle: (Optional) Starting angle for the first slice in degrees. Default is 90.
        hole_size: (Optional) Size of center hole (0-1). Default is 0.3.
        center_text: (Optional) Text to display in the center of the donut.

    Returns:
        Dictionary with public URL of the image, data summary, title, and instructions.

    **Example usage**:
    - Simple: ["Category A", "Category A", "Category B", "Category B", "Category B"]
    - With center text: center_text="Total: 100"
    """

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
        # Validate input data
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

        # Count occurrences of each unique value
        value_counts = Counter(filtered_data)

        # Extract labels and values
        labels = list(value_counts.keys())
        values = list(value_counts.values())

        # Create figure
        fig, ax = plt.subplots(figsize=(width, height))

        # Calculate percentages for legend
        total = sum(values)
        percentages = [(value / total * 100) for value in values]

        # Create labels with percentages for legend
        legend_labels = []
        for i, (label, pct) in enumerate(zip(labels, percentages)):
            if show_percentages:
                legend_labels.append(f"{label} ({pct:.1f}%)")
            else:
                legend_labels.append(label)

        # For small slices (less than 5%), don't show percentage on the slice itself
        def autopct_func(pct):
            if pct < 5.0:  # Small slices - don't show percentage on slice
                return ""
            return f"{pct:.1f}%"

        autopct = autopct_func if show_percentages else None

        # Create donut chart
        if show_percentages:
            wedges, texts, autotexts = ax.pie(
                values,
                labels=None,  # Remove labels from chart
                autopct=autopct,
                startangle=start_angle,
                colors=colors,
                pctdistance=0.85,
                wedgeprops=dict(width=1 - hole_size),  # Creates the donut hole
            )
        else:
            wedges, texts = ax.pie(
                values,
                labels=None,  # Remove labels from chart
                autopct=autopct,
                startangle=start_angle,
                colors=colors,
                wedgeprops=dict(width=1 - hole_size),  # Creates the donut hole
            )
            autotexts = None

        # Add center text if provided
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

        # Add legend outside the donut chart
        ax.legend(
            wedges,
            legend_labels,
            title="Categories",
            loc="center left",
            bbox_to_anchor=(1, 0, 0.5, 1),
            fontsize=10,
        )

        # Set title if provided
        if title:
            ax.set_title(title, fontsize=16, fontweight="bold", pad=20)

        # Ensure equal aspect ratio for circular donut
        ax.axis("equal")

        # Improve text readability for percentages shown on slices
        if show_percentages and autotexts:
            for autotext in autotexts:
                autotext.set_color("white")
                autotext.set_fontweight("bold")
                autotext.set_fontsize(9)

        # Adjust layout to accommodate legend
        plt.subplots_adjust(left=0.1, right=0.75)
        plt.tight_layout()

        # Save to bytes buffer
        img_buffer = io.BytesIO()
        plt.savefig(
            img_buffer,
            format="png",
            dpi=300,
            bbox_inches="tight",
            facecolor="white",
            edgecolor="none",
        )

        # Close the figure to free memory
        plt.close(fig)

        # Prepare data summary
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

        # Upload to S3
        upload_result = upload_image_to_s3(img_buffer)

        return {
            "public_url": upload_result["public_url"],
            "data_summary": data_summary,
            "title": title or "Donut Chart",
            "instructions": f"The donut chart has been uploaded to S3 at {upload_result['public_url']}. The data_summary provides details about the chart contents including counts and percentages.",
        }

    except Exception as e:
        await ctx.debug(f"Error creating donut chart: {str(e)}")
        return {
            "error": f"Failed to create donut chart: {str(e)}",
            "s3_bucket": None,
            "s3_key": None,
            "public_url": None,
        }


@mcp.tool()
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
    """
    Creates a heatmap image and returns the public URL of the image.

    **When to use**: Visualizing intensity/density in a 2D grid, showing patterns in
    categorical or continuous data, correlation matrices, or any matrix-like data.

    **Use cases**:
    - Correlation matrix between variables
    - Activity by hour and day of week
    - Performance metrics across regions and time periods
    - Sales data by product and month
    - Any 2D grid data visualization

    Parameters:
        ctx: Internal MCP context (do not supply manually).
        data: Dictionary containing the heatmap data. Format options:
            Matrix format: {"matrix": [[1, 2, 3], [4, 5, 6]], "x_labels": ["A", "B", "C"], "y_labels": ["X", "Y"]}
            Pivot format: {"x": ["A", "A", "B"], "y": ["X", "Y", "X"], "values": [1, 2, 3]}
            Raw format: {"raw_data": [{"x": "A", "y": "X", "value": 1}, ...]}
        title: (Optional) Title for the heatmap.
        x_label: (Optional) Label for the x-axis.
        y_label: (Optional) Label for the y-axis.
        colormap: (Optional) Colormap name (viridis, plasma, inferno, magma, etc.). Default is "viridis".
        width: (Optional) Width of the figure in inches. Default is 12.
        height: (Optional) Height of the figure in inches. Default is 8.
        show_values: (Optional) Whether to show values in cells. Default is True.
        value_format: (Optional) Format string for values. Default is ".2f".

    Returns:
        Dictionary with public URL of the image, data summary, title, and instructions.

    **Example usage**:
    - Correlation: {"matrix": [[1, 0.8, 0.6], [0.8, 1, 0.4], [0.6, 0.4, 1]], "x_labels": ["A", "B", "C"], "y_labels": ["A", "B", "C"]}
    - Activity: {"x": ["Mon", "Tue", "Wed"], "y": ["9AM", "10AM", "11AM"], "values": [10, 15, 8, 12, 20, 5, 18, 25, 12]}
    """

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
        # Validate input data
        if not data or not isinstance(data, dict):
            raise ValueError("Data must be a non-empty dictionary")

        # Process different data formats
        matrix = None
        x_labels = None
        y_labels = None

        if "matrix" in data:
            # Matrix format
            matrix = np.array(data["matrix"])
            x_labels = data.get(
                "x_labels", [f"Col {i}" for i in range(matrix.shape[1])]
            )
            y_labels = data.get(
                "y_labels", [f"Row {i}" for i in range(matrix.shape[0])]
            )
        elif "raw_data" in data:
            # Raw data format
            if isinstance(data["raw_data"], list) and data["raw_data"]:
                if isinstance(data["raw_data"][0], dict):
                    df = pd.DataFrame(data["raw_data"])
                    if not all(col in df.columns for col in ["x", "y", "value"]):
                        raise ValueError(
                            "Raw data must have 'x', 'y', and 'value' columns"
                        )

                    # Create pivot table
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
            # Pivot format
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

        # Create figure
        fig, ax = plt.subplots(figsize=(width, height))

        # create heatmap
        im = ax.imshow(matrix, cmap=colormap, aspect="auto")

        # Set ticks and labels
        ax.set_xticks(np.arange(len(x_labels)))
        ax.set_yticks(np.arange(len(y_labels)))
        ax.set_xticklabels(x_labels)
        ax.set_yticklabels(y_labels)

        # Rotate the tick labels for better readability
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right", rotation_mode="anchor")

        # Add colorbar
        cbar = ax.figure.colorbar(im, ax=ax)

        # Add values in cells if requested
        if show_values:
            for i in range(len(y_labels)):
                for j in range(len(x_labels)):
                    value = matrix[i, j]
                    # Choose text color based on value intensity
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

        # Set labels and title
        if x_label:
            ax.set_xlabel(x_label)
        if y_label:
            ax.set_ylabel(y_label)
        if title:
            ax.set_title(title, fontsize=16, fontweight="bold", pad=20)

        # Improve layout
        plt.tight_layout()

        # Save to bytes buffer
        img_buffer = io.BytesIO()
        plt.savefig(
            img_buffer,
            format="png",
            dpi=300,
            bbox_inches="tight",
            facecolor="white",
            edgecolor="none",
        )

        # Close the figure to free memory
        plt.close(fig)

        # Prepare data summary
        data_summary = {
            "matrix_shape": matrix.shape,
            "x_categories": len(x_labels),
            "y_categories": len(y_labels),
            "value_range": [float(matrix.min()), float(matrix.max())],
            "mean_value": float(matrix.mean()),
            "colormap": colormap,
            "values_shown": show_values,
        }

        # Upload to S3
        upload_result = upload_image_to_s3(img_buffer)

        return {
            "public_url": upload_result["public_url"],
            "data_summary": data_summary,
            "title": title or "Heatmap",
            "instructions": f"The heatmap has been uploaded to S3 at {upload_result['public_url']}. The data_summary provides details about the matrix dimensions and value ranges.",
        }

    except Exception as e:
        await ctx.debug(f"Error creating heatmap: {str(e)}")
        return {
            "error": f"Failed to create heatmap: {str(e)}",
            "s3_bucket": None,
            "s3_key": None,
            "public_url": None,
        }


@mcp.tool()
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
    """
    Creates an area chart image and returns the public URL of the image.

    **When to use**: Showing cumulative totals over time, emphasizing volume/magnitude,
    displaying part-to-whole relationships over a continuous axis, or showing trends
    with emphasis on the area under the curve.

    **Use cases**:
    - Revenue breakdown by product over time
    - Population growth by region
    - Cumulative sales targets vs actual
    - Budget allocation changes over periods
    - Any time series where total volume matters

    Parameters:
        ctx: Internal MCP context (do not supply manually).
        data: Dictionary containing the area chart data. Format options:
            Time series: {"x": [1, 2, 3, 4], "Series1": [10, 15, 12, 18], "Series2": [5, 8, 10, 7]}
            Single series: {"x": [1, 2, 3, 4], "y": [10, 15, 12, 18]}
            From raw data: {"raw_data": [{"x": 1, "series": "A", "value": 10}, ...]}
        title: (Optional) Title for the area chart.
        x_label: (Optional) Label for the x-axis.
        y_label: (Optional) Label for the y-axis.
        colors: (Optional) List of color names/hex codes for the areas.
        width: (Optional) Width of the figure in inches. Default is 12.
        height: (Optional) Height of the figure in inches. Default is 8.
        chart_style: (Optional) "stacked" or "overlapping". Default is "stacked".
        alpha: (Optional) Transparency of areas (0-1). Default is 0.7.
        show_markers: (Optional) Whether to show markers on lines. Default is False.

    Returns:
        Dictionary with public URL of the image, data summary, title, and instructions.

    **Example usage**:
    - Revenue: {"months": [1,2,3,4], "Product A": [100,120,110,130], "Product B": [80,90,85,95]}
    - Single: {"time": [1,2,3,4], "values": [10,15,12,18]}
    - Stacked vs overlapping styles for different visual emphasis
    """

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
        # Validate input data
        if not data or not isinstance(data, dict):
            raise ValueError("Data must be a non-empty dictionary")

        # Process different data formats
        x_data = None
        series_data = {}

        if "raw_data" in data:
            # Handle raw data format
            if isinstance(data["raw_data"], list) and data["raw_data"]:
                if isinstance(data["raw_data"][0], dict):
                    df = pd.DataFrame(data["raw_data"])
                    if not all(col in df.columns for col in ["x", "series", "value"]):
                        raise ValueError(
                            "Raw data must have 'x', 'series', and 'value' columns"
                        )

                    # Pivot data
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
            # Handle structured data
            # Find x-axis data
            x_keys = ["x", "time", "dates", "months", "years", "categories"]
            for key in x_keys:
                if key in data:
                    x_data = data[key]
                    break

            if x_data is None:
                raise ValueError(
                    "Data must contain x-axis data (x, time, dates, months, years, or categories)"
                )

            # Find y-axis series
            for key, values in data.items():
                if key not in x_keys and isinstance(values, list):
                    if len(values) != len(x_data):
                        raise ValueError(
                            f"Series '{key}' length ({len(values)}) doesn't match x-axis length ({len(x_data)})"
                        )
                    series_data[key] = values

        if not x_data or not series_data:
            raise ValueError("No valid data found for plotting")

        # Create figure
        fig, ax = plt.subplots(figsize=(width, height))

        # Set up colors
        if colors is None:
            colors = plt.cm.Set3(range(len(series_data)))
        elif len(colors) < len(series_data):
            # Extend colors if not enough provided
            default_colors = plt.cm.Set3(range(len(series_data)))
            colors.extend(default_colors[len(colors) :])

        # Prepare x-axis data for plotting
        x_plot = list(range(len(x_data))) if isinstance(x_data[0], str) else x_data

        if chart_style == "stacked":
            # Stacked area chart
            y_stacked = np.zeros(len(x_plot))

            for i, (series_name, y_values) in enumerate(series_data.items()):
                # Convert to numpy array for easier manipulation
                y_array = np.array(y_values)

                # Fill area
                ax.fill_between(
                    x_plot,
                    y_stacked,
                    y_stacked + y_array,
                    alpha=alpha,
                    color=colors[i],
                    label=series_name,
                )

                # Add line on top if markers requested
                if show_markers:
                    ax.plot(
                        x_plot,
                        y_stacked + y_array,
                        marker="o",
                        markersize=4,
                        color=colors[i],
                        linewidth=1,
                    )

                # Update the stacked base for next series
                y_stacked += y_array

        else:  # overlapping
            # Overlapping area chart
            for i, (series_name, y_values) in enumerate(series_data.items()):
                ax.fill_between(
                    x_plot, 0, y_values, alpha=alpha, color=colors[i], label=series_name
                )

                # Add line on top if markers requested
                if show_markers:
                    ax.plot(
                        x_plot,
                        y_values,
                        marker="o",
                        markersize=4,
                        color=colors[i],
                        linewidth=2,
                    )

        # Set x-axis labels
        if isinstance(x_data[0], str):
            ax.set_xticks(range(len(x_data)))
            ax.set_xticklabels(
                x_data, rotation=45 if len(max(x_data, key=len)) > 8 else 0
            )

        # Add legend if multiple series
        if len(series_data) > 1:
            ax.legend()

        # Set labels and title
        if x_label:
            ax.set_xlabel(x_label)
        if y_label:
            ax.set_ylabel(y_label)
        if title:
            ax.set_title(title, fontsize=16, fontweight="bold", pad=20)

        # Add grid for better readability
        ax.grid(True, alpha=0.3)

        # Improve layout
        plt.tight_layout()

        # Save to bytes buffer
        img_buffer = io.BytesIO()
        plt.savefig(
            img_buffer,
            format="png",
            dpi=300,
            bbox_inches="tight",
            facecolor="white",
            edgecolor="none",
        )

        # Close the figure to free memory
        plt.close(fig)

        # Prepare data summary
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

        # Upload to S3
        upload_result = upload_image_to_s3(img_buffer)

        return {
            "public_url": upload_result["public_url"],
            "data_summary": data_summary,
            "title": title or "Area Chart",
            "instructions": f"The area chart has been uploaded to S3 at {upload_result['public_url']}. The data_summary provides details about the chart configuration and data.",
        }

    except Exception as e:
        await ctx.debug(f"Error creating area chart: {str(e)}")
        return {
            "error": f"Failed to create area chart: {str(e)}",
            "s3_bucket": None,
            "s3_key": None,
            "public_url": None,
        }


# @mcp.tool()
# async def get_video_volunteers_community_engagement_insights(
#     ctx: Context,
#     start_date: Optional[str] = None,
#     end_date: Optional[str] = None,
#     city: Optional[str] = None,
#     district: Optional[str] = None,
#     state: Optional[str] = None,
# ) -> list[dict]:
#     """
#     Returns insights on video topics with most community engagement for Video Volunteers data.

#     This tool fetches all events matching the Video Volunteers (VV) data filter:
#       - Sub category is 'Citizen Initiatives'
#       - Hours invested is 0 (or 0.0 or '0.000')
#       - Location is set (not null)
#       - Optional filters: start_date, end_date, city, district, state

#     The output is a list of dicts with 'issue_addressed' and 'people_impacted' for each row where both are non-empty.

#     Parameters:
#         ctx: Internal MCP context (do not supply manually).
#         start_date: (Optional, format: DD/MM/YYYY) Start date for filtering event creation.
#         end_date: (Optional, format: DD/MM/YYYY) End date for filtering event creation.
#         city: (Optional) Filter by city.
#         district: (Optional) Filter by district.
#         state: (Optional) Filter by state.

#     Returns:
#         List of dicts: [{"issue_addressed": str, "people_impacted": str}, ...]
#     """

#     conn: asyncpg.Connection = await get_db_connection()

#     filters = [
#         "e.subcategory = 'Citizen Initiatives'",
#         "(e.hours_invested = 0 OR e.hours_invested = 0.0 OR e.hours_invested = '0.000')",
#         "e.location IS NOT NULL",
#     ]

#     if start_date:
#         try:
#             start_dt = datetime.strptime(start_date, "%d/%m/%Y")
#             filters.append(f"e.creation >= '{start_dt.date().isoformat()}'")
#         except Exception:
#             pass
#     if end_date:
#         try:
#             end_dt = datetime.strptime(end_date, "%d/%m/%Y")
#             filters.append(f"e.creation <= '{end_dt.date().isoformat()}'")
#         except Exception:
#             pass
#     if city:
#         filters.append(f"l.city = '{city}'")
#     if district:
#         filters.append(f"l.district = '{district}'")
#     if state:
#         filters.append(f"l.state = '{state}'")

#     where_clause = " AND ".join(filters)

#     query = f"""
#         SELECT e.description
#         FROM tabEvents e
#         LEFT JOIN "tabLocation" l ON e.location = l.name
#         WHERE {where_clause}
#     """

#     rows = await conn.fetch(query)

#     # Regex patterns for extracting Issue(s) Addressed and People Impacted
#     issue_pattern = re.compile(
#         r"Issue\(s\) Addressed:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL
#     )
#     people_pattern = re.compile(
#         r"People Impacted:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL
#     )

#     results = []
#     for row in rows:
#         desc = row["description"] or ""
#         issue_match = issue_pattern.search(desc)
#         people_match = people_pattern.search(desc)
#         issue_val = issue_match.group(1).strip() if issue_match else ""
#         people_val = people_match.group(1).strip() if people_match else ""
#         # Only include if both are non-empty and not "Data unavailable"
#         if (
#             issue_val
#             and people_val
#             and issue_val.lower() != "data unavailable"
#             and people_val.lower() != "data unavailable"
#         ):
#             results.append(
#                 {
#                     "issue_addressed": issue_val,
#                     "people_impacted": people_val,
#                 }
#             )

#     await conn.close()
#     return results


# @mcp.tool()
# async def get_video_volunteers_deployment_impact_insights(
#     ctx: Context,
#     start_date: Optional[str] = None,
#     end_date: Optional[str] = None,
#     city: Optional[str] = None,
#     district: Optional[str] = None,
#     state: Optional[str] = None,
# ) -> list[dict]:
#     """
#     Returns insights on where to deploy community video volunteers for maximum impact.

#     This tool fetches all events matching the Video Volunteers (VV) data filter:
#       - Sub category is 'Citizen Initiatives'
#       - Hours invested is 0 (or 0.0 or '0.000')
#       - Location is set (not null)
#       - Optional filters: start_date, end_date, city, district, state

#     The output is a list of dicts with 'location' and 'people_impacted' for each row where both are non-empty.

#     Parameters:
#         ctx: Internal MCP context (do not supply manually).
#         start_date: (Optional, format: DD/MM/YYYY) Start date for filtering event creation.
#         end_date: (Optional, format: DD/MM/YYYY) End date for filtering event creation.
#         city: (Optional) Filter by city.
#         district: (Optional) Filter by district.
#         state: (Optional) Filter by state.

#     Returns:
#         List of dicts: [{"location": str, "people_impacted": str}, ...]
#     """

#     conn: asyncpg.Connection = await get_db_connection()

#     filters = [
#         "e.subcategory = 'Citizen Initiatives'",
#         "(e.hours_invested = 0 OR e.hours_invested = 0.0 OR e.hours_invested = '0.000')",
#         "e.location IS NOT NULL",
#     ]

#     if start_date:
#         try:
#             start_dt = datetime.strptime(start_date, "%d/%m/%Y")
#             filters.append(f"e.creation >= '{start_dt.date().isoformat()}'")
#         except Exception:
#             pass
#     if end_date:
#         try:
#             end_dt = datetime.strptime(end_date, "%d/%m/%Y")
#             filters.append(f"e.creation <= '{end_dt.date().isoformat()}'")
#         except Exception:
#             pass
#     if city:
#         filters.append(f"l.city = '{city}'")
#     if district:
#         filters.append(f"l.district = '{district}'")
#     if state:
#         filters.append(f"l.state = '{state}'")

#     where_clause = " AND ".join(filters)

#     query = f"""
#         SELECT e.description
#         FROM tabEvents e
#         LEFT JOIN "tabLocation" l ON e.location = l.name
#         WHERE {where_clause}
#     """

#     rows = await conn.fetch(query)

#     # Regex patterns for extracting Location and People Impacted
#     location_pattern = re.compile(r"Location:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL)
#     people_pattern = re.compile(
#         r"People Impacted:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL
#     )

#     results = []
#     for row in rows:
#         desc = row["description"] or ""
#         location_match = location_pattern.search(desc)
#         people_match = people_pattern.search(desc)
#         location_val = location_match.group(1).strip() if location_match else ""
#         people_val = people_match.group(1).strip() if people_match else ""
#         # Only include if both are non-empty and not "Data unavailable"
#         if (
#             location_val
#             and people_val
#             and location_val.lower() != "data unavailable"
#             and people_val.lower() != "data unavailable"
#         ):
#             results.append(
#                 {
#                     "location": location_val,
#                     "people_impacted": people_val,
#                 }
#             )

#     await conn.close()
#     return results


# @mcp.tool()
# async def get_video_volunteers_steps_effectiveness_insights(
#     ctx: Context,
#     start_date: Optional[str] = None,
#     end_date: Optional[str] = None,
#     city: Optional[str] = None,
#     district: Optional[str] = None,
#     state: Optional[str] = None,
# ) -> list[dict]:
#     """
#     Returns insights on which "Steps Taken" combinations are most effective for different issue types.

#     This tool fetches all events matching the Video Volunteers (VV) data filter:
#       - Sub category is 'Citizen Initiatives'
#       - Hours invested is 0 (or 0.0 or '0.000')
#       - Location is set (not null)
#       - Optional filters: start_date, end_date, city, district, state

#     The output is a list of dicts with 'steps_taken', 'issues_addressed', and 'people_impacted'
#     for each row where all three fields are non-empty.

#     Parameters:
#         ctx: Internal MCP context (do not supply manually).
#         start_date: (Optional, format: DD/MM/YYYY) Start date for filtering event creation.
#         end_date: (Optional, format: DD/MM/YYYY) End date for filtering event creation.
#         city: (Optional) Filter by city.
#         district: (Optional) Filter by district.
#         state: (Optional) Filter by state.

#     Returns:
#         List of dicts: [{"steps_taken": str, "issues_addressed": str, "people_impacted": str}, ...]
#     """

#     conn: asyncpg.Connection = await get_db_connection()

#     filters = [
#         "e.subcategory = 'Citizen Initiatives'",
#         "(e.hours_invested = 0 OR e.hours_invested = 0.0 OR e.hours_invested = '0.000')",
#         "e.location IS NOT NULL",
#     ]

#     if start_date:
#         try:
#             start_dt = datetime.strptime(start_date, "%d/%m/%Y")
#             filters.append(f"e.creation >= '{start_dt.date().isoformat()}'")
#         except Exception:
#             pass
#     if end_date:
#         try:
#             end_dt = datetime.strptime(end_date, "%d/%m/%Y")
#             filters.append(f"e.creation <= '{end_dt.date().isoformat()}'")
#         except Exception:
#             pass
#     if city:
#         filters.append(f"l.city = '{city}'")
#     if district:
#         filters.append(f"l.district = '{district}'")
#     if state:
#         filters.append(f"l.state = '{state}'")

#     where_clause = " AND ".join(filters)

#     query = f"""
#         SELECT e.description
#         FROM tabEvents e
#         LEFT JOIN "tabLocation" l ON e.location = l.name
#         WHERE {where_clause}
#     """

#     rows = await conn.fetch(query)

#     # Regex patterns for extracting Steps Taken, Issue(s) Addressed, and People Impacted
#     steps_pattern = re.compile(r"Steps Taken:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL)
#     issues_pattern = re.compile(
#         r"Issue\(s\) Addressed:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL
#     )
#     people_pattern = re.compile(
#         r"People Impacted:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL
#     )

#     results = []
#     for row in rows:
#         desc = row["description"] or ""
#         steps_match = steps_pattern.search(desc)
#         issues_match = issues_pattern.search(desc)
#         people_match = people_pattern.search(desc)

#         steps_val = steps_match.group(1).strip() if steps_match else ""
#         issues_val = issues_match.group(1).strip() if issues_match else ""
#         people_val = people_match.group(1).strip() if people_match else ""

#         # Only include if all three fields are non-empty and not "Data unavailable"
#         if (
#             steps_val
#             and issues_val
#             and people_val
#             and steps_val.lower() != "data unavailable"
#             and issues_val.lower() != "data unavailable"
#             and people_val.lower() != "data unavailable"
#         ):
#             results.append(
#                 {
#                     "steps_taken": steps_val,
#                     "issues_addressed": issues_val,
#                     "people_impacted": people_val,
#                 }
#             )

#     await conn.close()
#     return results


# @mcp.tool()
# async def get_video_volunteers_root_causes(
#     ctx: Context,
#     start_date: Optional[str] = None,
#     end_date: Optional[str] = None,
#     city: Optional[str] = None,
#     district: Optional[str] = None,
#     state: Optional[str] = None,
# ) -> list[dict]:
#     """
#     Returns insights on top root causes from Video Volunteers data.

#     This tool fetches all events matching the Video Volunteers (VV) data filter:
#       - Sub category is 'Citizen Initiatives'
#       - Hours invested is 0 (or 0.0 or '0.000')
#       - Location is set (not null)
#       - Optional filters: start_date, end_date, city, district, state

#     The output is a list of dicts with 'root_cause' for each row where the field is non-empty.

#     Parameters:
#         ctx: Internal MCP context (do not supply manually).
#         start_date: (Optional, format: DD/MM/YYYY) Start date for filtering event creation.
#         end_date: (Optional, format: DD/MM/YYYY) End date for filtering event creation.
#         city: (Optional) Filter by city.
#         district: (Optional) Filter by district.
#         state: (Optional) Filter by state.

#     Returns:
#         List of dicts: [{"root_cause": str}, ...]
#     """

#     conn: asyncpg.Connection = await get_db_connection()

#     filters = [
#         "e.subcategory = 'Citizen Initiatives'",
#         "(e.hours_invested = 0 OR e.hours_invested = 0.0 OR e.hours_invested = '0.000')",
#         "e.location IS NOT NULL",
#     ]

#     if start_date:
#         try:
#             start_dt = datetime.strptime(start_date, "%d/%m/%Y")
#             filters.append(f"e.creation >= '{start_dt.date().isoformat()}'")
#         except Exception:
#             pass
#     if end_date:
#         try:
#             end_dt = datetime.strptime(end_date, "%d/%m/%Y")
#             filters.append(f"e.creation <= '{end_dt.date().isoformat()}'")
#         except Exception:
#             pass
#     if city:
#         filters.append(f"l.city = '{city}'")
#     if district:
#         filters.append(f"l.district = '{district}'")
#     if state:
#         filters.append(f"l.state = '{state}'")

#     where_clause = " AND ".join(filters)

#     query = f"""
#         SELECT e.description
#         FROM tabEvents e
#         LEFT JOIN "tabLocation" l ON e.location = l.name
#         WHERE {where_clause}
#     """

#     rows = await conn.fetch(query)

#     # Regex pattern for extracting Root Cause
#     root_cause_pattern = re.compile(
#         r"Root Cause:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL
#     )

#     results = []
#     for row in rows:
#         desc = row["description"] or ""
#         root_cause_match = root_cause_pattern.search(desc)
#         root_cause_val = root_cause_match.group(1).strip() if root_cause_match else ""

#         # Only include if field is non-empty and not "Data unavailable"
#         if root_cause_val and root_cause_val.lower() != "data unavailable":
#             results.append(
#                 {
#                     "root_cause": root_cause_val,
#                 }
#             )

#     await conn.close()

#     if len(results) > 1000:
#         return random.sample(results, 1000)

#     return results


# @mcp.tool()
# async def get_video_volunteers_policy_failure_patterns(
#     ctx: Context,
#     start_date: Optional[str] = None,
#     end_date: Optional[str] = None,
#     city: Optional[str] = None,
#     district: Optional[str] = None,
#     state: Optional[str] = None,
# ) -> list[dict]:
#     """
#     Returns comprehensive data for analyzing policy failure patterns from Video Volunteers data.

#     This tool fetches all events matching the Video Volunteers (VV) data filter and extracts
#     multiple fields to identify patterns that indicate local/system policy failures:
#       - Sub category is 'Citizen Initiatives'
#       - Hours invested is 0 (or 0.0 or '0.000')
#       - Location is set (not null)
#       - Optional filters: start_date, end_date, city, district, state

#     Returns only data points where ALL fields are present and non-empty.
#     If more than 100 results, randomly samples 100 for analysis.

#     Parameters:
#         ctx: Internal MCP context (do not supply manually).
#         start_date: (Optional, format: DD/MM/YYYY) Start date for filtering event creation.
#         end_date: (Optional, format: DD/MM/YYYY) End date for filtering event creation.
#         city: (Optional) Filter by city.
#         district: (Optional) Filter by district.
#         state: (Optional) Filter by state.

#     Returns:
#         List of dicts with all policy-relevant fields:
#         [{"root_cause": str, "issues_addressed": str, "action_needed_from": str,
#           "related_govt_program": str, "issue_duration": str, "affected_groups": str,
#           "area_type": str, "extent_of_issue_spread": str}, ...]
#     """

#     conn: asyncpg.Connection = await get_db_connection()

#     filters = [
#         "e.subcategory = 'Citizen Initiatives'",
#         "(e.hours_invested = 0 OR e.hours_invested = 0.0 OR e.hours_invested = '0.000')",
#         "e.location IS NOT NULL",
#     ]

#     if start_date:
#         try:
#             start_dt = datetime.strptime(start_date, "%d/%m/%Y")
#             filters.append(f"e.creation >= '{start_dt.date().isoformat()}'")
#         except Exception:
#             pass
#     if end_date:
#         try:
#             end_dt = datetime.strptime(end_date, "%d/%m/%Y")
#             filters.append(f"e.creation <= '{end_dt.date().isoformat()}'")
#         except Exception:
#             pass
#     if city:
#         filters.append(f"l.city = '{city}'")
#     if district:
#         filters.append(f"l.district = '{district}'")
#     if state:
#         filters.append(f"l.state = '{state}'")

#     where_clause = " AND ".join(filters)

#     query = f"""
#         SELECT e.description
#         FROM tabEvents e
#         LEFT JOIN "tabLocation" l ON e.location = l.name
#         WHERE {where_clause}
#     """

#     rows = await conn.fetch(query)

#     # Regex patterns for extracting all policy-relevant fields
#     root_cause_pattern = re.compile(
#         r"Root Cause:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL
#     )
#     issues_pattern = re.compile(
#         r"Issue\(s\) Addressed:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL
#     )
#     action_needed_pattern = re.compile(
#         r"Action Needed From:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL
#     )
#     govt_program_pattern = re.compile(
#         r"Related Govt Program:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL
#     )
#     issue_duration_pattern = re.compile(
#         r"Issue Duration:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL
#     )
#     affected_groups_pattern = re.compile(
#         r"Affected Groups:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL
#     )
#     area_type_pattern = re.compile(r"Area Type:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL)
#     extent_pattern = re.compile(
#         r"Extent of issue spread:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL
#     )

#     results = []
#     for row in rows:
#         desc = row["description"] or ""

#         # Extract all fields
#         root_cause_match = root_cause_pattern.search(desc)
#         issues_match = issues_pattern.search(desc)
#         action_needed_match = action_needed_pattern.search(desc)
#         govt_program_match = govt_program_pattern.search(desc)
#         issue_duration_match = issue_duration_pattern.search(desc)
#         affected_groups_match = affected_groups_pattern.search(desc)
#         area_type_match = area_type_pattern.search(desc)
#         extent_match = extent_pattern.search(desc)

#         # Get values and strip whitespace
#         root_cause_val = root_cause_match.group(1).strip() if root_cause_match else ""
#         issues_val = issues_match.group(1).strip() if issues_match else ""
#         action_needed_val = (
#             action_needed_match.group(1).strip() if action_needed_match else ""
#         )
#         govt_program_val = (
#             govt_program_match.group(1).strip() if govt_program_match else ""
#         )
#         issue_duration_val = (
#             issue_duration_match.group(1).strip() if issue_duration_match else ""
#         )
#         affected_groups_val = (
#             affected_groups_match.group(1).strip() if affected_groups_match else ""
#         )
#         area_type_val = area_type_match.group(1).strip() if area_type_match else ""
#         extent_val = extent_match.group(1).strip() if extent_match else ""

#         # Only include if ALL fields are non-empty and not "Data unavailable"
#         if (
#             root_cause_val
#             and root_cause_val.lower() != "data unavailable"
#             and issues_val
#             and issues_val.lower() != "data unavailable"
#             and action_needed_val
#             and action_needed_val.lower() != "data unavailable"
#             and govt_program_val
#             and govt_program_val.lower() != "data unavailable"
#             and issue_duration_val
#             and issue_duration_val.lower() != "data unavailable"
#             and affected_groups_val
#             and affected_groups_val.lower() != "data unavailable"
#             and area_type_val
#             and area_type_val.lower() != "data unavailable"
#             and extent_val
#             and extent_val.lower() != "data unavailable"
#         ):
#             results.append(
#                 {
#                     "root_cause": root_cause_val,
#                     "issues_addressed": issues_val,
#                     "action_needed_from": action_needed_val,
#                     "related_govt_program": govt_program_val,
#                     "issue_duration": issue_duration_val,
#                     "affected_groups": affected_groups_val,
#                     "area_type": area_type_val,
#                     "extent_of_issue_spread": extent_val,
#                 }
#             )

#     await conn.close()

#     # Sample 100 if more than 100 results
#     if len(results) > 1000:
#         return random.sample(results, 1000)

#     return results


if __name__ == "__main__":
    mcp.run(transport="sse")
