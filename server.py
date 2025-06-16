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


mcp = FastMCP("SamaajData MCP server", host="0.0.0.0")


async def get_db_connection():
    conn = await asyncpg.connect(DATABASE_URL)
    return conn


@mcp.resource("data://event_categories")
async def event_categories() -> list[str]:
    with open("event_categories.json", "r") as f:
        values = json.load(f)
        return [value["name"] for value in values]


# @mcp.tool()
# async def get_all_fire_incidents(
#     ctx: Context, start_date: Optional[str] = None, end_date: Optional[str] = None
# ) -> list[dict]:
#     """Return all fire incidents of stubble burning (optionally, within a date range) in the format of:
#         Year,District,Tehsil / Block,Satellite,Latitude,Longitude,ACQ_DATE,ACQ_TIME,Day / Night,Fire Power (W/m2)

#     The raw data is from 1/10/2021 to 9/11/2021.

#     Args:
#         start_date: The start date of the date range. Format: DD/MM/YYYY
#         end_date: The end date of the date range.

#     Example:
#     ```
#     get_fire_incidents_by_date_range("22/11/2021", "22/11/2021")
#     ```
#     """
#     await ctx.debug(f"start_date: {start_date}")
#     await ctx.debug(str(df["ACQ_DATE"].values))

#     if start_date:
#         start = pd.to_datetime(start_date, dayfirst=True)
#     if end_date:
#         end = pd.to_datetime(end_date, dayfirst=True)

#     filtered_df = df[df["ACQ_DATE"].notna()]

#     if start_date:
#         filtered_df = filtered_df[filtered_df["ACQ_DATE"] >= start]
#     if end_date:
#         filtered_df = filtered_df[filtered_df["ACQ_DATE"] <= end]

#     filtered_df["ACQ_DATE"] = filtered_df["ACQ_DATE"].dt.strftime("%d/%m/%Y")

#     return json.dumps(filtered_df.to_dict(orient="records"))


@mcp.tool()
async def get_valid_categories(ctx: Context) -> str:
    """
    Returns the list of valid issue/action/event categories that can be used as a filter. If the user query requires filtering for specific category of issue/action/event, use this method to get the list of valid categories to pick from
    """
    with open("event_categories.json", "r") as f:
        values = json.load(f)
        return ", ".join([value["name"] for value in values])


@mcp.tool()
async def get_valid_subcategories(ctx: Context) -> str:
    """
    Returns the list of valid issue/action/event subcategories that can be used as a filter. If the user query requires filtering for specific subcategory of issue/action/event, use this method to get the list of valid subcategories to pick from. Only pick this if the category is also picked and the user query requires further filtering beyond category.
    """
    with open("event_subcategories.json", "r") as f:
        values = json.load(f)
        return ", ".join([value["name"] for value in values])


@mcp.tool()
async def get_valid_event_types(ctx: Context) -> str:
    """
    Returns the list of valid issue/action/event types that can be used as a filter. If the user query requires filtering for specific type of issue/action/event, use this method to get the list of valid types to pick from.
    """
    with open("event_types.json", "r") as f:
        values = json.load(f)
        return ", ".join([value["name"] for value in values])


@mcp.tool()
async def test_db_connection(ctx: Context) -> list[str]:
    conn: asyncpg.Connection = await get_db_connection()

    rows = await conn.fetch("SELECT title FROM tabEvents LIMIT 10")
    await conn.close()
    return [row["title"] for row in rows]


@mcp.tool()
async def get_event_points_for_area(
    ctx: Context,
    aggregation_level: Literal["district", "state", "hobli", "grama_panchayath"],
    value: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    category: Optional[str] = None,
    subcategory: Optional[str] = None,
    type: Optional[str] = None,
) -> list[dict]:
    """
    Returns all individual event points (latitude, longitude) for a given area and filters.

    Parameters:
        ctx: Internal MCP context (do not supply manually).
        aggregation_level: Geographic level to match ("district", "state", "hobli", or "grama_panchayath").
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
        A list of tuples containing (latitude, longitude)
    """

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

    await ctx.debug([(row["latitude"], row["longitude"]) for row in rows])

    return [(row["latitude"], row["longitude"]) for row in rows]


@mcp.tool()
async def get_spatial_data_clusters(
    ctx: Context,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    category: Optional[str] = None,
    subcategory: Optional[str] = None,
    type: Optional[str] = None,
    aggregate_by: Optional[
        Literal["point", "district", "state", "hobli", "grama_panchayath"]
    ] = None,
    aggregation_value: Optional[str] = None,
) -> dict:
    """
    Returns the spatial data on all issues based on filters and desired aggregation level. If aggregate_by is "point", the raw lat/lon for each issue is returned. Otherwise, the data is grouped by the aggregation level and all lower levels in the hierarchy.

    Aggregation hierarchy (from highest to lowest):
    - state -> district -> hobli -> grama_panchayath -> point

    When you specify an aggregation level, you get data for that level and all lower levels.
    For example:
    - aggregate_by="state" returns: state, district, hobli, grama_panchayath aggregations
    - aggregate_by="district" returns: district, hobli, grama_panchayath aggregations
    - aggregate_by="hobli" returns: hobli, grama_panchayath aggregations

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
            - "hobli": grouped by hobli and lower levels (grama_panchayath)
            - "grama_panchayath": grouped by grama_panchayath only
            - empty if not provided by the user
        aggregation_value: (Optional) Value to aggregate by. If not provided, all values are returned. If provided, only the data for that value is returned.

    Returns:
        A dictionary with:
        - data: Dictionary with aggregation levels as keys and their respective data as values
            * If "point": {"point": [{"latitude": float, "longitude": float, "count": int, "location": str}, ...]}
            * Otherwise: {"state": [...], "district": [...], "hobli": [...], "grama_panchayath": [...]}
        - meta: Metadata including time range and whether defaults were applied
        - instructions: Message for LLM to explain assumptions and guide the user
    """

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


if __name__ == "__main__":
    mcp.run(transport="sse")
