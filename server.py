import json
from typing import Optional
from mcp.server.fastmcp import FastMCP,Context
from mcp.server.fastmcp.prompts import base

import pandas as pd
import logging
from datetime import datetime

# Load your dataset (you might want to replace this with your actual CSV loading logic)
df = pd.read_csv("stubble_data.csv")
df["ACQ_DATE"] = pd.to_datetime(df["ACQ_DATE"], format="%d/%m/%Y", errors="coerce")

mcp = FastMCP("Punjab Fire Analysis Server")

# ========== Resources ==========

@mcp.resource("data://fires")
def raw_fire_data() -> str:
    """Return the full fire dataset as a CSV string."""
    return df.to_csv(index=False)

# ========== Tools ==========

@mcp.tool()
async def get_all_fire_incidents(ctx: Context, start_date: Optional[str] = None, end_date: Optional[str] = None) -> list[dict]:
    """Return all fire incidents of stubble burning (optionally, within a date range) in the format of:
        Year,District,Tehsil / Block,Satellite,Latitude,Longitude,ACQ_DATE,ACQ_TIME,Day / Night,Fire Power (W/m2)

    The raw data is from 1/10/2021 to 9/11/2021.
    
    Args:
        start_date: The start date of the date range. Format: DD/MM/YYYY
        end_date: The end date of the date range.

    Example:
    ```
    get_fire_incidents_by_date_range("22/11/2021", "22/11/2021")
    ```
    """
    await ctx.debug(f"start_date: {start_date}")
    await ctx.debug(str(df['ACQ_DATE'].values))
    

    if start_date:
        start = pd.to_datetime(start_date, dayfirst=True)
    if end_date:
        end = pd.to_datetime(end_date, dayfirst=True)

    filtered_df = df[df['ACQ_DATE'].notna()]

    if start_date:
        filtered_df = filtered_df[filtered_df["ACQ_DATE"] >= start]
    if end_date:
        filtered_df = filtered_df[filtered_df["ACQ_DATE"] <= end]

    filtered_df["ACQ_DATE"] = filtered_df["ACQ_DATE"].dt.strftime("%d/%m/%Y")

    return json.dumps(filtered_df.to_dict(orient="records"))

@mcp.tool()
def aggregate_fire_power_by_day(start_date: str, end_date: str, district: str = None) -> dict:
    """Aggregate total fire power per day within date range and optional district."""
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    filtered = df[(df["ACQ_DATE"] >= start) & (df["ACQ_DATE"] <= end)]
    if district:
        filtered = filtered[filtered["District"] == district.upper()]
    result = filtered.groupby("ACQ_DATE")["Fire Power (W/m2)"].sum().to_dict()
    return {k.strftime("%Y-%m-%d"): v for k, v in result.items()}

@mcp.tool()
def count_incidents_by_satellite() -> dict:
    """Count incidents by satellite."""
    return df["Satellite"].value_counts().to_dict()

@mcp.tool()
def top_fire_days(n: int = 5, district: str = None) -> list[dict]:
    """Top N days with highest total fire power."""
    data = df.copy()
    if district:
        data = data[data["District"] == district.upper()]
    summary = data.groupby("ACQ_DATE")["Fire Power (W/m2)"].sum().nlargest(n)
    return [{"date": d.strftime("%Y-%m-%d"), "total_power": p} for d, p in summary.items()]

@mcp.tool()
def analyze_night_vs_day() -> dict:
    """Compare count and average fire power during day vs night."""
    counts = df["Day / Night"].value_counts().to_dict()
    means = df.groupby("Day / Night")["Fire Power (W/m2)"].mean().round(2).to_dict()
    return {"counts": counts, "average_power": means}

# ========== Prompts ==========

@mcp.prompt()
def trend_analysis_prompt(start_date: str, end_date: str, district: str = "MANSA") -> str:
    return f"""Based on the stubble burning data in {district} from {start_date} to {end_date}, summarize the trend in fire activity over time. 
Highlight any peaks, changes in intensity, or sudden drops in fire power."""

@mcp.prompt()
def satellite_comparison_prompt() -> str:
    return "Compare how many incidents were recorded by the AQUA and S-NPP satellites and whether one is significantly more active or sensitive."

@mcp.prompt()
def impact_assessment_prompt(region: str = "MANSA", time_period: str = "Nov 2021") -> str:
    return f"""Has the fire activity in {region} during {time_period} shown signs of reduction due to government policy interventions like mulching subsidies?"""

@mcp.prompt()
def awareness_story_prompt(region: str = "SARDULGARH", person_name: str = "Hardeep") -> str:
    return f"""Using data from {region}, generate a compelling story similar to Hardeep’s about someone who observed a spike in stubble burning, educated the community, and promoted mulching as an alternative."""


# @mcp.tool()
# async def reload_tools(ctx: Context) -> str:
#     """Notify clients that the tools list has changed."""
#     await ctx.session.send_notification(
#         NotificationRequestParams(method="tools/list_changed")
#     )
#     return "Client notified of tool updates."


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
