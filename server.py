import json
import os
from typing import Optional, Literal
from mcp.server.fastmcp import FastMCP, Context
from mcp.server.fastmcp.prompts import base
import asyncpg
import pandas as pd
import json
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
async def get_valid_categories(ctx: Context) -> list[str]:
    """
    Returns the list of valid event categories that can be used as a filter.
    """
    with open("event_categories.json", "r") as f:
        return json.load(f)


@mcp.tool()
async def get_valid_subcategories(ctx: Context) -> list[str]:
    """
    Returns the list of valid event subcategories that can be used as a filter.
    """
    with open("event_subcategories.json", "r") as f:
        return json.load(f)


@mcp.tool()
async def get_valid_event_types(ctx: Context) -> list[str]:
    """
    Returns the list of valid event types that can be used as a filter.
    """
    with open("event_types.json", "r") as f:
        return json.load(f)


@mcp.tool()
async def test_db_connection(ctx: Context) -> list[str]:
    await ctx.debug(f"{DATABASE_URL}")
    conn: asyncpg.Connection = await get_db_connection()
    await ctx.debug(f"here2")
    rows = await conn.fetch("SELECT title FROM tabEvents LIMIT 10")
    return [row["title"] for row in rows]


@mcp.tool()
async def get_event_points_for_area(
    ctx: Context,
    aggregate_by: Literal["district", "state", "hobli", "grama_panchayath"],
    value: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    category: Optional[str] = None,
    subcategory: Optional[str] = None,
    type: Optional[str] = None,
) -> list[dict]:
    await ctx.debug(f"here")
    """
    Returns all individual event points (latitude, longitude) for a given area and filters.

    Parameters:
        ctx: Internal MCP context (do not supply manually).
        aggregate_by: Geographic level to match ("district", "state", "hobli", or "grama_panchayath").
        value: Name of the area to match (e.g., "Ludhiana" if aggregate_by="district").
        start_date: (Optional, format: DD/MM/YYYY) Start date for filtering event creation.
        end_date: (Optional, format: DD/MM/YYYY) End date for filtering event creation.
        category: (Optional) Filter to include only events matching this category.
                  Use values returned by get_valid_categories().
        subcategory: (Optional) Filter to include only events matching this subcategory.
                     Use values returned by get_valid_subcategories().
        type: (Optional) Filter to include only events matching this event type.
              Use values returned by get_valid_event_types().

    Returns:
        A list of dictionaries containing:
        - latitude, longitude, title, category, subcategory, type, status, description
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
        f"l.{aggregate_by} = '{value}'",
    ]
    if category:
        filters.append(f"e.category = '{category}'")
    if subcategory:
        filters.append(f"e.subcategory = '{subcategory}'")
    if type:
        filters.append(f"e.type = '{type}'")

    where_clause = " AND ".join(filters)

    query = f"""
    SELECT 
        e.latitude::float AS latitude,
        e.longitude::float AS longitude,
        e.title,
        e.category,
        e.subcategory,
        e.type,
        e.status,
        e.description
    FROM tabEvents e
    LEFT JOIN tabLocation l ON e.location = l.name
    WHERE 
        e.latitude IS NOT NULL
        AND e.longitude IS NOT NULL
        AND e.latitude ~ '^[0-9.+-]+$'
        AND e.longitude ~ '^[0-9.+-]+$'
        AND {where_clause}
    LIMIT 500
    """

    await ctx.debug(f"Query:\n{query}")
    conn: asyncpg.Connection = await get_db_connection()
    rows = await conn.fetch(query)

    return [
        {
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "title": row["title"],
            "category": row["category"],
            "subcategory": row["subcategory"],
            "type": row["type"],
            "status": row["status"],
            "description": row["description"],
        }
        for row in rows
    ]


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
