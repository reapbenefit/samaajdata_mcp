import json
import os
from typing import Optional, Literal
from mcp.server.fastmcp import FastMCP, Context
from mcp.server.fastmcp.prompts import base
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

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")


# @asynccontextmanager
# async def lifespan(app: FastMCP):
#     async with app.default_lifespan():
#         conn = await asyncpg.connect(DATABASE_URL)
#         try:
#             yield {"db": conn}  # inject into context
#         finally:
#             await conn.close()


parser = argparse.ArgumentParser()
parser.add_argument("--port", action="store", type=int, default=8000)
args = parser.parse_args()


mcp = FastMCP("SamaajData MCP server", host="0.0.0.0", port=args.port)


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


@mcp.tool()
async def extract_data_from_samaajdata(
    ctx: Context,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    category: Optional[str] = None,
    subcategory: Optional[str] = None,
    type: Optional[str] = None,
    aggregate_by: Optional[
        Literal["point", "district", "state", "hobli_name", "grama_panchayath"]
    ] = None,
    aggregation_value: Optional[str] = None,
) -> dict:
    """
    Returns the spatial data on all issues based on filters and desired aggregation level from Samaajdata. If aggregate_by is "point", the raw lat/lon for each issue is returned. Otherwise, the data is grouped by the aggregation level and all lower levels in the hierarchy.

    Aggregation hierarchy (from highest to lowest):
    - state -> district -> hobli -> grama_panchayath -> point

    When you specify an aggregation level, you get data for that level and all lower levels.
    For example:
    - aggregate_by="state" returns: state, district, hobli, grama_panchayath aggregations
    - aggregate_by="district" returns: district, hobli, grama_panchayath aggregations
    - aggregate_by="hobli_name" returns: hobli_name, grama_panchayath aggregations

    For any data requested for video volunteers, always apply the following base filters on top of which other filters can be applied:
    - event.subcategory = 'Citizen Initiatives'
    - (event.hours_invested = 0 OR event.hours_invested = 0.0 OR event.hours_invested = '0.000')
    - event.location IS NOT NULL

    Parameters:
        ctx: Internal MCP context (do not supply manually).
        start_date: (Optional, format: DD/MM/YYYY) Start date for filtering events by creation date.
        end_date: (Optional, format: DD/MM/YYYY) End date for filtering events by creation date.
        category: (Optional) Filter to include only events matching this category. If given, the value must be one of the values returned by get_valid_categories(). Only pick the most relevant category.
        subcategory: (Optional) Filter to include only events matching this subcategory. If given, the value must be one of the values returned by get_valid_subcategories(). Only pick the most relevant subcategory. Only pick this if the category is also picked and the user query requires further filtering beyond category.
        type: (Optional) Filter to include only events matching this event type. If given, the value must be one of the values returned by get_valid_event_types(). Only pick the most relevant type.
        aggregate_by: (Optional) How to group events for clustering.
            - "point": raw lat/lon for each issue (no aggregation)
            - "state": grouped by state and all lower levels (district, hobli, grama_panchayath)
            - "district": grouped by district and lower levels (hobli, grama_panchayath)
            - "hobli_name": grouped by hobli_name and lower levels (grama_panchayath)
            - "grama_panchayath": grouped by grama_panchayath only
            - empty if not provided by the user
        aggregation_value: (Optional) Value to aggregate by. If not provided, all values are returned. If provided, only the data for that value is returned.

    Returns:
        A dictionary with:
        - data: Dictionary with aggregation levels as keys and their respective data as values
            * If "point": {"point": [{"latitude": float, "longitude": float, "count": int, "location": str}, ...]}
            * Otherwise: {"state": [...], "district": [...], "hobli_name": [...], "grama_panchayath": [...]}
        - meta: Metadata including time range and whether defaults were applied
        - instructions: Message for LLM to explain assumptions and guide the user
    """

    await insert_query(
        "extract_data_from_samaajdata",
        {
            "start_date": start_date,
            "end_date": end_date,
            "category": category,
            "subcategory": subcategory,
            "type": type,
            "aggregate_by": aggregate_by,
            "aggregation_value": aggregation_value,
        },
    )

    # Define aggregation hierarchy
    hierarchy = ["state", "district", "hobli_name", "grama_panchayath", "point"]

    # Default date range handling
    if start_date:
        start = pd.to_datetime(start_date, dayfirst=True)
    if end_date:
        end = pd.to_datetime(end_date, dayfirst=True)
    if not start_date:
        start = datetime(2000, 1, 1)
    if not end_date:
        end = datetime.today()

    if not aggregate_by:
        aggregate_by = "district"
        default_agg_applied = True
    else:
        default_agg_applied = False

    # Build base filters
    filters = []
    if start_date:
        filters.append(f"e.creation >= '{start.date().isoformat()}'")
    if end_date:
        filters.append(f"e.creation <= '{end.date().isoformat()}'")
    if category:
        filters.append(f"e.category = '{category}'")
    if subcategory:
        filters.append(f"e.subcategory = '{subcategory}'")
    if type:
        filters.append(f"e.type = '{type}'")

    base_where_clause = " AND ".join(filters) if filters else "1=1"

    conn: asyncpg.Connection = await get_db_connection()

    result_data = {}

    if aggregate_by == "point":
        # Handle point aggregation separately
        where_clause = base_where_clause
        if aggregation_value:
            # For point aggregation, aggregation_value could be used to filter by location
            where_clause += f" AND (l.state = '{aggregation_value}' OR l.district = '{aggregation_value}' OR l.hobli_name = '{aggregation_value}' OR l.grama_panchayath = '{aggregation_value}')"

        query = f"""
        SELECT 
            e.latitude::float AS latitude,
            e.longitude::float AS longitude,
            e.title AS title,
            e.category AS category,
            e.subcategory AS subcategory,
            e.type AS type,
            COUNT(*) AS count,
            COALESCE(l.city, l.district, l.state, e.location, l.hobli_name, l.grama_panchayath) AS location
        FROM "tabEvents" e
        LEFT JOIN "tabLocation" l ON e.location = l.name
        WHERE 
            e.latitude IS NOT NULL 
            AND e.longitude IS NOT NULL 
            AND e.latitude ~ '^[0-9.+-]+$'
            AND e.longitude ~ '^[0-9.+-]+$'
            AND {where_clause}
        GROUP BY e.latitude, e.longitude, location, l.city, l.district, l.state, l.hobli_name, l.grama_panchayath, e.title, e.category, e.subcategory, e.type
        ORDER BY count DESC
        """

        await ctx.debug(f"Point Query:\n{query}")
        rows = await conn.fetch(query)

        result_data["point"] = [
            {
                "latitude": row["latitude"],
                "longitude": row["longitude"],
                "count": row["count"],
                "location": row["location"],
                "title": row["title"],
                "category": row["category"],
                "subcategory": row["subcategory"],
                "type": row["type"],
            }
            for row in rows
        ]
    else:
        # Handle multi-level aggregation
        # Get the index of the requested aggregation level
        start_index = hierarchy.index(aggregate_by)

        # Get all levels from the requested level down to grama_panchayath
        levels_to_aggregate = hierarchy[start_index:-1]  # Exclude "point"

        for level in levels_to_aggregate:
            where_clause = base_where_clause
            if aggregation_value:
                where_clause += f" AND l.{aggregate_by} = '{aggregation_value}'"

            query = f"""
            SELECT 
                l.{level} AS location, 
                COUNT(*) AS count
            FROM "tabEvents" e
            LEFT JOIN "tabLocation" l ON e.location = l.name
            WHERE 
                e.latitude IS NOT NULL 
                AND e.longitude IS NOT NULL 
                AND e.latitude ~ '^[0-9.+-]+$'
                AND e.longitude ~ '^[0-9.+-]+$'
                AND {where_clause}
                AND l.{level} IS NOT NULL
                AND l.{level} != ''
            GROUP BY l.{level}
            ORDER BY count DESC
            LIMIT 100
            """

            await ctx.debug(f"{level.capitalize()} Query:\n{query}")
            rows = await conn.fetch(query)

            result_data[level] = [
                {"location": row["location"], "count": row["count"]}
                for row in rows
                if row["location"]
            ]

    await conn.close()

    instructions = (
        "Surface the assumptions used (like time range = past 30 days, aggregation = district) to the user if any.\n"
        "Encourage them to try more specific queries like:\n"
        "- 'Show me events in March by hobli'\n"
        "- 'Cluster sanitation issues in Karnataka by grama panchayat'\n"
        "- 'Where are fire incidents concentrated this year?'\n"
        "If the number of issues is very high (if it will exceed your context limit) and the user wants you to do further analysis on the raw data points, ask them to use the filters to reduce the number of issues first.\n"
        "Empty location values mean the location data wasn't available for that field."
    )

    output = {
        "data": result_data,
        "meta": {
            "defaults_applied": {
                "aggregation": default_agg_applied,
            },
            "time_range_used": {
                "start": start.date().isoformat(),
                "end": end.date().isoformat(),
            },
            "aggregation_used": aggregate_by,
            "aggregation_levels_returned": list(result_data.keys()),
        },
    }

    if default_agg_applied:
        output["instructions"] = instructions

    return output


@mcp.tool()
async def get_event_trend_over_time_from_samaajdata(
    ctx: Context,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    category: Optional[str] = None,
    subcategory: Optional[str] = None,
    type: Optional[str] = None,
    interval: Literal["month", "week", "year"] = "month",
    filter_date_type: Optional[Literal["year", "month"]] = None,
    filter_date_value: Optional[str] = None,
    aggregation_level: Optional[
        Literal["district", "state", "hobli_name", "grama_panchayath"]
    ] = None,
    aggregation_value: Optional[str] = None,
) -> dict:
    """
    Returns a time series of event counts to track trends over time, grouped by the given interval from Samaajdata.

    For any data requested for video volunteers, always apply the following base filters on top of which other filters can be applied:
    - event.subcategory = 'Citizen Initiatives'
    - (event.hours_invested = 0 OR event.hours_invested = 0.0 OR event.hours_invested = '0.000')
    - event.location IS NOT NULL

    Parameters:
        ctx: Internal MCP context (do not supply manually).
        start_date: (Optional, format: DD/MM/YYYY) Start date for filtering event creation date.
        end_date: (Optional, format: DD/MM/YYYY) End date for filtering event creation date.
        category: (Optional) Filter by event category.
                  Use values from get_valid_categories().
        subcategory: (Optional) Filter by event subcategory.
                     Use values from get_valid_subcategories().
        type: (Optional) Filter by event type.
              Use values from get_valid_event_types().
        interval: Time grouping level. One of "month" (default), "week", or "year".
        aggregation_level: (Optional) Geographic level to match ("district", "state", "hobli_name", or "grama_panchayath").
        aggregation_value: (Optional) Name of the area to match (e.g., "Ludhiana" if aggregation_level="district").
    Returns:
        A dictionary with:
        - trend: List of dicts with 'period' and 'count'
        - meta: Time range used and filters applied
        - instructions: Message for the LLM to explain results and suggest deeper analysis
    """

    await insert_query(
        "get_event_trend_over_time_from_samaajdata",
        {
            "start_date": start_date,
            "end_date": end_date,
            "category": category,
            "subcategory": subcategory,
            "type": type,
            "interval": interval,
            "filter_date_type": filter_date_type,
            "filter_date_value": filter_date_value,
            "aggregation_level": aggregation_level,
            "aggregation_value": aggregation_value,
        },
    )

    if start_date:
        start = pd.to_datetime(start_date, dayfirst=True)

    if end_date:
        end = pd.to_datetime(end_date, dayfirst=True)
    else:
        end = datetime.today()

    filters = []
    if start_date:
        filters.append(f"e.creation >= '{start.date().isoformat()}'")
    if end_date:
        filters.append(f"e.creation <= '{end.date().isoformat()}'")
    if category:
        filters.append(f"e.category = '{category}'")
    if subcategory:
        filters.append(f"e.subcategory = '{subcategory}'")
    if type:
        filters.append(f"e.type = '{type}'")
    if aggregation_level:
        filters.append(f"l.{aggregation_level} = '{aggregation_value}'")
    if filter_date_type and filter_date_value:
        if filter_date_type == "year":
            filters.append(f"EXTRACT(year FROM e.creation) = {filter_date_value}")
        elif filter_date_type == "month":
            filters.append(f"to_char(e.creation, 'YYYY-MM') = '{filter_date_value}'")

    where_clause = " AND ".join(filters)

    # Time truncation
    time_field = {
        "month": "to_char(date_trunc('month', e.creation), 'YYYY-MM')",
        "week": "to_char(date_trunc('week', e.creation), 'IYYY-IW')",
        "year": "to_char(date_trunc('year', e.creation), 'YYYY')",
    }[interval]

    await ctx.debug(f"where_clause: {where_clause}")

    query = f"""
    SELECT 
        {time_field} AS period,
        COUNT(*) AS count
    FROM tabEvents e
    LEFT JOIN "tabLocation" l ON e.location = l.name
    WHERE {where_clause}
    GROUP BY period
    ORDER BY period
    """

    await ctx.debug(f"Query:\n{query}")

    conn: asyncpg.Connection = await get_db_connection()
    rows = await conn.fetch(query)

    trend = [{"period": row["period"], "count": row["count"]} for row in rows]

    return {
        "trend": trend,
        "instructions": "Display this as a line or bar chart showing trends over time. Use the 'interval' field to choose the x-axis (month, week, year). You can ask follow-ups like: 'Break this down by location', 'Compare this with sanitation issues', 'Show me peak months of complaints'",
    }


@mcp.tool()
async def get_video_volunteers_data(
    ctx: Context,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    city: Optional[str] = None,
    district: Optional[str] = None,
    state: Optional[str] = None,
    aggregate_by: Optional[Literal["city", "district", "state"]] = None,
) -> list[dict]:
    """
    Returns data about Video Volunteers on SamaajData.
    This data refers to events where:
      - Sub category is 'Citizen Initiatives'
      - Hours invested is 0
      - Location is set (not null)
    Supports filtering by start/end date, city, district, state, and aggregation by city/district/state.
    """
    conn: asyncpg.Connection = await get_db_connection()

    filters = [
        "e.subcategory = 'Citizen Initiatives'",
        "(e.hours_invested = 0 OR e.hours_invested = 0.0 OR e.hours_invested = '0.000')",
        "e.location IS NOT NULL",
    ]

    if start_date:
        try:
            start_dt = datetime.strptime(start_date, "%d/%m/%Y")
            filters.append(f"e.creation >= '{start_dt.date().isoformat()}'")
        except Exception:
            pass
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, "%d/%m/%Y")
            filters.append(f"e.creation <= '{end_dt.date().isoformat()}'")
        except Exception:
            pass
    if city:
        filters.append(f"l.city = '{city}'")
    if district:
        filters.append(f"l.district = '{district}'")
    if state:
        filters.append(f"l.state = '{state}'")

    where_clause = " AND ".join(filters)

    select_fields = [
        "e.name",
        "e.category",
        "e.description",
        "e.subcategory",
        "e.type",
        "e.location",
        "l.district",
        "l.state",
        "e.creation",
    ]
    group_by_fields = []

    if aggregate_by == "city":
        select_fields = ["l.city", "COUNT(*) AS count"]
        group_by_fields = ["l.city"]
    elif aggregate_by == "district":
        select_fields = ["l.district", "COUNT(*) AS count"]
        group_by_fields = ["l.district"]
    elif aggregate_by == "state":
        select_fields = ["l.state", "COUNT(*) AS count"]
        group_by_fields = ["l.state"]

    select_clause = ", ".join(select_fields)
    group_by_clause = (
        f"GROUP BY {', '.join(group_by_fields)}" if group_by_fields else ""
    )

    query = f"""
    SELECT 
        {select_clause}
    FROM tabEvents e
    LEFT JOIN "tabLocation" l ON e.location = l.name
    WHERE {where_clause}
    {group_by_clause}
    LIMIT 100
    """

    await insert_query(
        "get_video_volunteers_data",
        {
            "start_date": start_date,
            "end_date": end_date,
            "city": city,
            "district": district,
            "state": state,
            "aggregate_by": aggregate_by,
        },
    )

    rows = await conn.fetch(query)

    if aggregate_by in ("city", "district", "state"):
        # Return aggregation
        return [
            {aggregate_by: row[aggregate_by], "count": row["count"]} for row in rows
        ]
    else:
        # Return as list of dicts
        return [
            {
                "name": row["name"],
                "category": row["category"],
                "description": row["description"],
                "subcategory": row["subcategory"],
                "type": row["type"],
                "location": row["location"],
                "district": row["district"],
                "state": row["state"],
                "creation": row["creation"].isoformat() if row["creation"] else None,
            }
            for row in rows
        ]


@mcp.tool()
async def get_video_volunteer_count(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    city: Optional[str] = None,
    district: Optional[str] = None,
    state: Optional[str] = None,
) -> dict:
    """
    Returns the count of video volunteers on SamaajData.
    """

    conn: asyncpg.Connection = await get_db_connection()

    where_clauses = []
    params = {}

    if start_date:
        where_clauses.append("e.creation >= $1")
        params["start_date"] = start_date
    if end_date:
        where_clauses.append("e.creation <= $2")
        params["end_date"] = end_date
    if city:
        where_clauses.append("l.city = $3")
        params["city"] = city
    if district:
        where_clauses.append("l.district = $4")
        params["district"] = district
    if state:
        where_clauses.append("l.state = $5")
        params["state"] = state

    # Apply the basic filters as above to get the whole set of VV data
    where_clauses.append("e.subcategory = 'Citizen Initiatives'")
    where_clauses.append(
        "(e.hours_invested = 0 OR e.hours_invested = 0.0 OR e.hours_invested = '0.000')"
    )
    where_clauses.append("e.location IS NOT NULL")

    where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

    query = f"""
        SELECT COUNT(*) as count
        FROM tabEvents e
        LEFT JOIN "tabLocation" l ON e.location = l.name
        WHERE {where_clause}
    """

    # The connection object 'conn' should be available in your context
    # Adjust parameter passing as per your DB driver
    values = []
    for key in ["start_date", "end_date", "city", "district", "state"]:
        if key in params:
            values.append(params[key])

    count_row = await conn.fetchrow(query, *values)
    return {"count": count_row["count"] if count_row else 0}


@mcp.tool()
async def get_trees_data_count(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
) -> dict:
    """
    Returns the count of trees data on SamaajData.
    """

    conn: asyncpg.Connection = await get_db_connection()

    where_clauses = []
    params = {}

    if start_date:
        where_clauses.append("em.creation >= $1")
        params["start_date"] = datetime.strptime(start_date, "%d/%m/%Y")
    if end_date:
        where_clauses.append("em.creation <= $2")
        params["end_date"] = datetime.strptime(end_date, "%d/%m/%Y")
    if city:
        where_clauses.append("em.city = $3")
        params["city"] = city
    if state:
        where_clauses.append("em.state = $5")
        params["state"] = state

    # Apply the basic filters to get the trees data
    where_clauses.append("em.event_category = 'Tree Tracking'")
    where_clauses.append(
        "(em.event_subcategory = 'Trees' OR em.event_subcategory = 'Tree Tracking')"
    )
    where_clauses.append("em.location_id IS NOT NULL")

    where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

    query = f"""
        SELECT COUNT(DISTINCT em.event_id) as count
        FROM "Events Metadata" em
        WHERE {where_clause}
    """

    # The connection object 'conn' should be available in your context
    # Adjust parameter passing as per your DB driver
    values = []
    for key in ["start_date", "end_date", "city", "district", "state"]:
        if key in params:
            values.append(params[key])

    count_row = await conn.fetchrow(query, *values)
    await conn.close()
    return {"count": count_row["count"] if count_row else 0}


@mcp.tool()
async def get_trees_data_metadata(
    ctx: Context,
) -> dict:
    """
    Returns the list of fields for all the trees data available to query on SamaajData.
    When any information is asked about trees data, use this tool to get the list of fields/data points that are available
    about trees in the database and then, based on the field names and field descriptions, decide if the user's query can be answered with the data available.
    """

    conn: asyncpg.Connection = await get_db_connection()

    # Apply the basic filters to get the trees data
    where_clauses = [
        "e.event_category = 'Tree Tracking'",
        "(e.event_subcategory = 'Trees' OR e.event_subcategory = 'Tree Tracking')",
        "e.location_id IS NOT NULL",
    ]

    where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

    query = f"""
        SELECT 
            e.field_name, 
            e.field_definition,
            ARRAY(
                SELECT DISTINCT em.field_value
                FROM "Events Metadata" em
                WHERE em.field_name = e.field_name
                  AND em.event_category = 'Tree Tracking'
                  AND (em.event_subcategory = 'Trees' OR em.event_subcategory = 'Tree Tracking')
                  AND em.location_id IS NOT NULL
                LIMIT 10
            ) AS example_values
        FROM "Events Metadata" e
        WHERE {where_clause}
        GROUP BY e.field_name, e.field_definition
    """

    rows = await conn.fetch(query)

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
async def get_trees_data_field_values(
    ctx: Context,
    field_name: str,
    aggregation_type: Optional[Literal["unique", "count"]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
) -> dict:
    """
    Returns all values for a given field from Trees data.

    Parameters:
        ctx: Internal MCP context (do not supply manually).
        field_name: (Required) The name of the field to get values for.
        aggregation_type: (Optional, Literal["unique", "count"]) The type of aggregation to perform on the field values.
        start_date: (Optional, format: DD/MM/YYYY) Start date for filtering event creation.
        end_date: (Optional, format: DD/MM/YYYY) End date for filtering event creation.
        city: (Optional) Filter by city.
        district: (Optional) Filter by district.
        state: (Optional) Filter by state.

    Returns:
        dict: {"values": [value_1, value_2, ...]} or {"values": {"value_1": count_1, "value_2": count_2, ...}}
    """

    conn: asyncpg.Connection = await get_db_connection()

    filters = [
        "em.event_category = 'Tree Tracking'",
        "(em.event_subcategory = 'Trees' OR em.event_subcategory = 'Tree Tracking')",
        "em.location_id IS NOT NULL",
        f"em.field_name = '{field_name}'",
    ]

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

    where_clause = " AND ".join(filters)

    query = f"""
        SELECT em.field_value
        FROM "Events Metadata" em
        WHERE {where_clause}
    """

    await ctx.debug(f"Query: {query}")

    rows = await conn.fetch(query)

    results = []
    for row in rows:
        results.append(row["field_value"])

    await conn.close()

    results = [result for result in results if result.strip()]

    await ctx.debug(f"aggregation_type: {aggregation_type}")

    if aggregation_type == "unique":
        results = list(set(results))
    elif aggregation_type == "count":
        from collections import Counter

        results = dict(Counter(results))

    if isinstance(results, list) and len(results) > 1000:
        results = random.sample(results, 1000)

    return {"values": results}


@mcp.tool()
async def get_video_volunteers_community_engagement_insights(
    ctx: Context,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    city: Optional[str] = None,
    district: Optional[str] = None,
    state: Optional[str] = None,
) -> list[dict]:
    """
    Returns insights on video topics with most community engagement for Video Volunteers data.

    This tool fetches all events matching the Video Volunteers (VV) data filter:
      - Sub category is 'Citizen Initiatives'
      - Hours invested is 0 (or 0.0 or '0.000')
      - Location is set (not null)
      - Optional filters: start_date, end_date, city, district, state

    The output is a list of dicts with 'issue_addressed' and 'people_impacted' for each row where both are non-empty.

    Parameters:
        ctx: Internal MCP context (do not supply manually).
        start_date: (Optional, format: DD/MM/YYYY) Start date for filtering event creation.
        end_date: (Optional, format: DD/MM/YYYY) End date for filtering event creation.
        city: (Optional) Filter by city.
        district: (Optional) Filter by district.
        state: (Optional) Filter by state.

    Returns:
        List of dicts: [{"issue_addressed": str, "people_impacted": str}, ...]
    """

    conn: asyncpg.Connection = await get_db_connection()

    filters = [
        "e.subcategory = 'Citizen Initiatives'",
        "(e.hours_invested = 0 OR e.hours_invested = 0.0 OR e.hours_invested = '0.000')",
        "e.location IS NOT NULL",
    ]

    if start_date:
        try:
            start_dt = datetime.strptime(start_date, "%d/%m/%Y")
            filters.append(f"e.creation >= '{start_dt.date().isoformat()}'")
        except Exception:
            pass
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, "%d/%m/%Y")
            filters.append(f"e.creation <= '{end_dt.date().isoformat()}'")
        except Exception:
            pass
    if city:
        filters.append(f"l.city = '{city}'")
    if district:
        filters.append(f"l.district = '{district}'")
    if state:
        filters.append(f"l.state = '{state}'")

    where_clause = " AND ".join(filters)

    query = f"""
        SELECT e.description
        FROM tabEvents e
        LEFT JOIN "tabLocation" l ON e.location = l.name
        WHERE {where_clause}
    """

    rows = await conn.fetch(query)

    # Regex patterns for extracting Issue(s) Addressed and People Impacted
    issue_pattern = re.compile(
        r"Issue\(s\) Addressed:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL
    )
    people_pattern = re.compile(
        r"People Impacted:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL
    )

    results = []
    for row in rows:
        desc = row["description"] or ""
        issue_match = issue_pattern.search(desc)
        people_match = people_pattern.search(desc)
        issue_val = issue_match.group(1).strip() if issue_match else ""
        people_val = people_match.group(1).strip() if people_match else ""
        # Only include if both are non-empty and not "Data unavailable"
        if (
            issue_val
            and people_val
            and issue_val.lower() != "data unavailable"
            and people_val.lower() != "data unavailable"
        ):
            results.append(
                {
                    "issue_addressed": issue_val,
                    "people_impacted": people_val,
                }
            )

    await conn.close()
    return results


@mcp.tool()
async def get_video_volunteers_deployment_impact_insights(
    ctx: Context,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    city: Optional[str] = None,
    district: Optional[str] = None,
    state: Optional[str] = None,
) -> list[dict]:
    """
    Returns insights on where to deploy community video volunteers for maximum impact.

    This tool fetches all events matching the Video Volunteers (VV) data filter:
      - Sub category is 'Citizen Initiatives'
      - Hours invested is 0 (or 0.0 or '0.000')
      - Location is set (not null)
      - Optional filters: start_date, end_date, city, district, state

    The output is a list of dicts with 'location' and 'people_impacted' for each row where both are non-empty.

    Parameters:
        ctx: Internal MCP context (do not supply manually).
        start_date: (Optional, format: DD/MM/YYYY) Start date for filtering event creation.
        end_date: (Optional, format: DD/MM/YYYY) End date for filtering event creation.
        city: (Optional) Filter by city.
        district: (Optional) Filter by district.
        state: (Optional) Filter by state.

    Returns:
        List of dicts: [{"location": str, "people_impacted": str}, ...]
    """

    conn: asyncpg.Connection = await get_db_connection()

    filters = [
        "e.subcategory = 'Citizen Initiatives'",
        "(e.hours_invested = 0 OR e.hours_invested = 0.0 OR e.hours_invested = '0.000')",
        "e.location IS NOT NULL",
    ]

    if start_date:
        try:
            start_dt = datetime.strptime(start_date, "%d/%m/%Y")
            filters.append(f"e.creation >= '{start_dt.date().isoformat()}'")
        except Exception:
            pass
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, "%d/%m/%Y")
            filters.append(f"e.creation <= '{end_dt.date().isoformat()}'")
        except Exception:
            pass
    if city:
        filters.append(f"l.city = '{city}'")
    if district:
        filters.append(f"l.district = '{district}'")
    if state:
        filters.append(f"l.state = '{state}'")

    where_clause = " AND ".join(filters)

    query = f"""
        SELECT e.description
        FROM tabEvents e
        LEFT JOIN "tabLocation" l ON e.location = l.name
        WHERE {where_clause}
    """

    rows = await conn.fetch(query)

    # Regex patterns for extracting Location and People Impacted
    location_pattern = re.compile(r"Location:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL)
    people_pattern = re.compile(
        r"People Impacted:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL
    )

    results = []
    for row in rows:
        desc = row["description"] or ""
        location_match = location_pattern.search(desc)
        people_match = people_pattern.search(desc)
        location_val = location_match.group(1).strip() if location_match else ""
        people_val = people_match.group(1).strip() if people_match else ""
        # Only include if both are non-empty and not "Data unavailable"
        if (
            location_val
            and people_val
            and location_val.lower() != "data unavailable"
            and people_val.lower() != "data unavailable"
        ):
            results.append(
                {
                    "location": location_val,
                    "people_impacted": people_val,
                }
            )

    await conn.close()
    return results


@mcp.tool()
async def get_video_volunteers_steps_effectiveness_insights(
    ctx: Context,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    city: Optional[str] = None,
    district: Optional[str] = None,
    state: Optional[str] = None,
) -> list[dict]:
    """
    Returns insights on which "Steps Taken" combinations are most effective for different issue types.

    This tool fetches all events matching the Video Volunteers (VV) data filter:
      - Sub category is 'Citizen Initiatives'
      - Hours invested is 0 (or 0.0 or '0.000')
      - Location is set (not null)
      - Optional filters: start_date, end_date, city, district, state

    The output is a list of dicts with 'steps_taken', 'issues_addressed', and 'people_impacted'
    for each row where all three fields are non-empty.

    Parameters:
        ctx: Internal MCP context (do not supply manually).
        start_date: (Optional, format: DD/MM/YYYY) Start date for filtering event creation.
        end_date: (Optional, format: DD/MM/YYYY) End date for filtering event creation.
        city: (Optional) Filter by city.
        district: (Optional) Filter by district.
        state: (Optional) Filter by state.

    Returns:
        List of dicts: [{"steps_taken": str, "issues_addressed": str, "people_impacted": str}, ...]
    """

    conn: asyncpg.Connection = await get_db_connection()

    filters = [
        "e.subcategory = 'Citizen Initiatives'",
        "(e.hours_invested = 0 OR e.hours_invested = 0.0 OR e.hours_invested = '0.000')",
        "e.location IS NOT NULL",
    ]

    if start_date:
        try:
            start_dt = datetime.strptime(start_date, "%d/%m/%Y")
            filters.append(f"e.creation >= '{start_dt.date().isoformat()}'")
        except Exception:
            pass
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, "%d/%m/%Y")
            filters.append(f"e.creation <= '{end_dt.date().isoformat()}'")
        except Exception:
            pass
    if city:
        filters.append(f"l.city = '{city}'")
    if district:
        filters.append(f"l.district = '{district}'")
    if state:
        filters.append(f"l.state = '{state}'")

    where_clause = " AND ".join(filters)

    query = f"""
        SELECT e.description
        FROM tabEvents e
        LEFT JOIN "tabLocation" l ON e.location = l.name
        WHERE {where_clause}
    """

    rows = await conn.fetch(query)

    # Regex patterns for extracting Steps Taken, Issue(s) Addressed, and People Impacted
    steps_pattern = re.compile(r"Steps Taken:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL)
    issues_pattern = re.compile(
        r"Issue\(s\) Addressed:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL
    )
    people_pattern = re.compile(
        r"People Impacted:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL
    )

    results = []
    for row in rows:
        desc = row["description"] or ""
        steps_match = steps_pattern.search(desc)
        issues_match = issues_pattern.search(desc)
        people_match = people_pattern.search(desc)

        steps_val = steps_match.group(1).strip() if steps_match else ""
        issues_val = issues_match.group(1).strip() if issues_match else ""
        people_val = people_match.group(1).strip() if people_match else ""

        # Only include if all three fields are non-empty and not "Data unavailable"
        if (
            steps_val
            and issues_val
            and people_val
            and steps_val.lower() != "data unavailable"
            and issues_val.lower() != "data unavailable"
            and people_val.lower() != "data unavailable"
        ):
            results.append(
                {
                    "steps_taken": steps_val,
                    "issues_addressed": issues_val,
                    "people_impacted": people_val,
                }
            )

    await conn.close()
    return results


@mcp.tool()
async def get_video_volunteers_root_causes(
    ctx: Context,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    city: Optional[str] = None,
    district: Optional[str] = None,
    state: Optional[str] = None,
) -> list[dict]:
    """
    Returns insights on top root causes from Video Volunteers data.

    This tool fetches all events matching the Video Volunteers (VV) data filter:
      - Sub category is 'Citizen Initiatives'
      - Hours invested is 0 (or 0.0 or '0.000')
      - Location is set (not null)
      - Optional filters: start_date, end_date, city, district, state

    The output is a list of dicts with 'root_cause' for each row where the field is non-empty.

    Parameters:
        ctx: Internal MCP context (do not supply manually).
        start_date: (Optional, format: DD/MM/YYYY) Start date for filtering event creation.
        end_date: (Optional, format: DD/MM/YYYY) End date for filtering event creation.
        city: (Optional) Filter by city.
        district: (Optional) Filter by district.
        state: (Optional) Filter by state.

    Returns:
        List of dicts: [{"root_cause": str}, ...]
    """

    conn: asyncpg.Connection = await get_db_connection()

    filters = [
        "e.subcategory = 'Citizen Initiatives'",
        "(e.hours_invested = 0 OR e.hours_invested = 0.0 OR e.hours_invested = '0.000')",
        "e.location IS NOT NULL",
    ]

    if start_date:
        try:
            start_dt = datetime.strptime(start_date, "%d/%m/%Y")
            filters.append(f"e.creation >= '{start_dt.date().isoformat()}'")
        except Exception:
            pass
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, "%d/%m/%Y")
            filters.append(f"e.creation <= '{end_dt.date().isoformat()}'")
        except Exception:
            pass
    if city:
        filters.append(f"l.city = '{city}'")
    if district:
        filters.append(f"l.district = '{district}'")
    if state:
        filters.append(f"l.state = '{state}'")

    where_clause = " AND ".join(filters)

    query = f"""
        SELECT e.description
        FROM tabEvents e
        LEFT JOIN "tabLocation" l ON e.location = l.name
        WHERE {where_clause}
    """

    rows = await conn.fetch(query)

    # Regex pattern for extracting Root Cause
    root_cause_pattern = re.compile(
        r"Root Cause:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL
    )

    results = []
    for row in rows:
        desc = row["description"] or ""
        root_cause_match = root_cause_pattern.search(desc)
        root_cause_val = root_cause_match.group(1).strip() if root_cause_match else ""

        # Only include if field is non-empty and not "Data unavailable"
        if root_cause_val and root_cause_val.lower() != "data unavailable":
            results.append(
                {
                    "root_cause": root_cause_val,
                }
            )

    await conn.close()

    if len(results) > 1000:
        return random.sample(results, 1000)

    return results


@mcp.tool()
async def get_video_volunteers_policy_failure_patterns(
    ctx: Context,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    city: Optional[str] = None,
    district: Optional[str] = None,
    state: Optional[str] = None,
) -> list[dict]:
    """
    Returns comprehensive data for analyzing policy failure patterns from Video Volunteers data.

    This tool fetches all events matching the Video Volunteers (VV) data filter and extracts
    multiple fields to identify patterns that indicate local/system policy failures:
      - Sub category is 'Citizen Initiatives'
      - Hours invested is 0 (or 0.0 or '0.000')
      - Location is set (not null)
      - Optional filters: start_date, end_date, city, district, state

    Returns only data points where ALL fields are present and non-empty.
    If more than 100 results, randomly samples 100 for analysis.

    Parameters:
        ctx: Internal MCP context (do not supply manually).
        start_date: (Optional, format: DD/MM/YYYY) Start date for filtering event creation.
        end_date: (Optional, format: DD/MM/YYYY) End date for filtering event creation.
        city: (Optional) Filter by city.
        district: (Optional) Filter by district.
        state: (Optional) Filter by state.

    Returns:
        List of dicts with all policy-relevant fields:
        [{"root_cause": str, "issues_addressed": str, "action_needed_from": str,
          "related_govt_program": str, "issue_duration": str, "affected_groups": str,
          "area_type": str, "extent_of_issue_spread": str}, ...]
    """

    conn: asyncpg.Connection = await get_db_connection()

    filters = [
        "e.subcategory = 'Citizen Initiatives'",
        "(e.hours_invested = 0 OR e.hours_invested = 0.0 OR e.hours_invested = '0.000')",
        "e.location IS NOT NULL",
    ]

    if start_date:
        try:
            start_dt = datetime.strptime(start_date, "%d/%m/%Y")
            filters.append(f"e.creation >= '{start_dt.date().isoformat()}'")
        except Exception:
            pass
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, "%d/%m/%Y")
            filters.append(f"e.creation <= '{end_dt.date().isoformat()}'")
        except Exception:
            pass
    if city:
        filters.append(f"l.city = '{city}'")
    if district:
        filters.append(f"l.district = '{district}'")
    if state:
        filters.append(f"l.state = '{state}'")

    where_clause = " AND ".join(filters)

    query = f"""
        SELECT e.description
        FROM tabEvents e
        LEFT JOIN "tabLocation" l ON e.location = l.name
        WHERE {where_clause}
    """

    rows = await conn.fetch(query)

    # Regex patterns for extracting all policy-relevant fields
    root_cause_pattern = re.compile(
        r"Root Cause:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL
    )
    issues_pattern = re.compile(
        r"Issue\(s\) Addressed:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL
    )
    action_needed_pattern = re.compile(
        r"Action Needed From:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL
    )
    govt_program_pattern = re.compile(
        r"Related Govt Program:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL
    )
    issue_duration_pattern = re.compile(
        r"Issue Duration:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL
    )
    affected_groups_pattern = re.compile(
        r"Affected Groups:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL
    )
    area_type_pattern = re.compile(r"Area Type:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL)
    extent_pattern = re.compile(
        r"Extent of issue spread:\s*(.*?)<br>", re.IGNORECASE | re.DOTALL
    )

    results = []
    for row in rows:
        desc = row["description"] or ""

        # Extract all fields
        root_cause_match = root_cause_pattern.search(desc)
        issues_match = issues_pattern.search(desc)
        action_needed_match = action_needed_pattern.search(desc)
        govt_program_match = govt_program_pattern.search(desc)
        issue_duration_match = issue_duration_pattern.search(desc)
        affected_groups_match = affected_groups_pattern.search(desc)
        area_type_match = area_type_pattern.search(desc)
        extent_match = extent_pattern.search(desc)

        # Get values and strip whitespace
        root_cause_val = root_cause_match.group(1).strip() if root_cause_match else ""
        issues_val = issues_match.group(1).strip() if issues_match else ""
        action_needed_val = (
            action_needed_match.group(1).strip() if action_needed_match else ""
        )
        govt_program_val = (
            govt_program_match.group(1).strip() if govt_program_match else ""
        )
        issue_duration_val = (
            issue_duration_match.group(1).strip() if issue_duration_match else ""
        )
        affected_groups_val = (
            affected_groups_match.group(1).strip() if affected_groups_match else ""
        )
        area_type_val = area_type_match.group(1).strip() if area_type_match else ""
        extent_val = extent_match.group(1).strip() if extent_match else ""

        # Only include if ALL fields are non-empty and not "Data unavailable"
        if (
            root_cause_val
            and root_cause_val.lower() != "data unavailable"
            and issues_val
            and issues_val.lower() != "data unavailable"
            and action_needed_val
            and action_needed_val.lower() != "data unavailable"
            and govt_program_val
            and govt_program_val.lower() != "data unavailable"
            and issue_duration_val
            and issue_duration_val.lower() != "data unavailable"
            and affected_groups_val
            and affected_groups_val.lower() != "data unavailable"
            and area_type_val
            and area_type_val.lower() != "data unavailable"
            and extent_val
            and extent_val.lower() != "data unavailable"
        ):
            results.append(
                {
                    "root_cause": root_cause_val,
                    "issues_addressed": issues_val,
                    "action_needed_from": action_needed_val,
                    "related_govt_program": govt_program_val,
                    "issue_duration": issue_duration_val,
                    "affected_groups": affected_groups_val,
                    "area_type": area_type_val,
                    "extent_of_issue_spread": extent_val,
                }
            )

    await conn.close()

    # Sample 100 if more than 100 results
    if len(results) > 1000:
        return random.sample(results, 1000)

    return results


mcp.run(transport="sse")
