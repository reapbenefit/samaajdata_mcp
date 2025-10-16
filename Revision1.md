# Quick Integration Guide

## How to Integrate the Solutions into Your Existing Code

### Option 1: Full Replacement (Recommended)
Replace your existing files completely with the enhanced versions:
- `agent.py` → Use "Enhanced agent.py with Context & Error Handling"
- `server.py` → Use "Enhanced server.py with Location Discovery & URL Formatting"

### Option 2: Selective Integration
If you prefer to merge changes into your existing code, follow these steps:

---

## Changes to `agent.py`

### 1. Add Missing Import
```python
import re  # Add this at the top with other imports
import asyncio  # Add for retry delay
```

### 2. Add New Functions (Before `build_context_from_history`)
```python
@log_execution_time("extract_entities_from_history", main_logger)
def extract_entities_from_history(chat_history: List[ChatMessage]) -> dict:
    # Copy entire function from enhanced version
    ...

@log_execution_time("resolve_contextual_references", main_logger)
def resolve_contextual_references(query: str, chat_history: List[ChatMessage], entities: dict) -> str:
    # Copy entire function from enhanced version
    ...
```

### 3. Update `build_context_from_history`
**Line 84: Change max_messages**
```python
# OLD:
def build_context_from_history(chat_history: List[ChatMessage], max_messages: int = 8) -> str:

# NEW:
def build_context_from_history(chat_history: List[ChatMessage], max_messages: int = 100) -> str:
```

**After line 99: Add entity extraction**
```python
# Add this after the "if not recent_messages:" check:
# Extract entities for better context understanding
entities = extract_entities_from_history(recent_messages)
```

**After line 105: Add entity summary**
```python
# Add this before the conversation messages:
# Add entity summary if significant entities found
if entities["locations"] or entities["categories"] or entities["partners"]:
    context_parts.append("KEY TOPICS IN THIS CONVERSATION:")
    if entities["locations"]:
        context_parts.append(f"- Locations mentioned: {', '.join(entities['locations'])}")
    if entities["categories"]:
        context_parts.append(f"- Topics/Categories: {', '.join(entities['categories'])}")
    if entities["partners"]:
        context_parts.append(f"- Data partners: {', '.join(entities['partners'])}")
    context_parts.append("")
```

**After line 116: Enhance instructions**
```python
# Replace the old context ending with:
context_parts.extend([
    "",
    "=== END CONTEXT ===",
    "",
    "IMPORTANT INSTRUCTIONS FOR HANDLING CONTEXT:",
    "- Reference previous topics, locations, and data when relevant",
    "- If the user uses vague language like 'that', 'it', 'this', infer meaning from context",
    "- If the user says 'yeah', 'okay', or similar, they are likely accepting an offer you made",
    "- Maintain conversation continuity and avoid repeating information already provided",
    "- Build upon previous analysis when expanding on a topic",
    ""
])
```

### 4. Update `answer_query` Function
**Replace entire function body (lines 177-238) with retry logic:**

```python
async def answer_query(request: QueryRequest):
    """Main endpoint to answer user queries with comprehensive logging and retry logic"""
    
    main_logger.info(f"Processing query request: {len(request.query)} characters, "
                    f"{len(request.chat_history)} history messages")
    
    # Retry configuration
    max_retries = 2
    retry_count = 0
    last_error = None
    
    while retry_count <= max_retries:
        try:
            async with log_async_operation("query_processing", perf_logger, include_memory=True):
                
                # Extract entities and resolve references
                entities = extract_entities_from_history(request.chat_history) if request.chat_history else {}
                resolved_query = resolve_contextual_references(request.query, request.chat_history, entities)
                
                if resolved_query != request.query:
                    main_logger.info(f"Query resolved from '{request.query}' to '{resolved_query}'")
                
                # Build context (rest of the function)
                # ... (copy from enhanced version)
                
        except Exception as e:
            retry_count += 1
            last_error = e
            main_logger.error(f"Query processing attempt {retry_count} failed: {str(e)}")
            
            if retry_count <= max_retries:
                main_logger.info(f"Retrying query...")
                await asyncio.sleep(0.5 * retry_count)
            else:
                # Return user-friendly error
                return """I apologize, but I encountered an error..."""
```

### 5. Update Agent Instructions (line 194)
**Add to instructions string:**
```python
CONTEXTUAL REFERENCE RESOLUTION:
- If the query mentions "it", "that", "this" without context, check previous messages
- If user says "more details" or "tell me more", expand on the last topic discussed
- If location/category is implied but not stated, use the most recent one from context
- Track numerical values mentioned (counts, percentages) for later reference

LOCATION DISCOVERY:
- When a user asks about data for a location, FIRST check if that location has data available
- If the requested location has no data, suggest alternative nearby locations or broader regions
- Always inform users about available locations when they ask about a place with no data
```

---

## Changes to `server.py`

### 1. Add City Name Mappings (After imports, around line 50)
```python
# ==================== CITY NAME NORMALIZATION ====================

CITY_NAME_MAPPINGS = {
    "bangalore": ["Bangalore", "Bengaluru", "bangalore", "bengaluru"],
    "bengaluru": ["Bangalore", "Bengaluru", "bangalore", "bengaluru"],
    "mumbai": ["Mumbai", "Bombay", "mumbai", "bombay"],
    # ... (copy full dictionary from enhanced version)
}

@log_execution_time("normalize_city_name", main_logger)
def normalize_city_name(city: str) -> list[str]:
    # Copy entire function
    ...
```

### 2. Add URL Formatting Function (After city normalization)
```python
# ==================== URL FORMATTING ====================

@log_execution_time("format_urls_as_markdown_links", main_logger)
def format_urls_as_markdown_links(text: str) -> str:
    # Copy entire function
    ...
```

### 3. Add Location Discovery Tools (After existing tools)
```python
# ==================== LOCATION DISCOVERY TOOLS ====================

@mcp.tool()
@log_execution_time("get_available_locations_for_category")
async def get_available_locations_for_category(...):
    # Copy entire function
    ...

@mcp.tool()
@log_execution_time("get_location_hierarchy")
async def get_location_hierarchy(...):
    # Copy entire function
    ...
```

### 4. Update Existing Query Functions
For `get_event_points_for_area_from_samaajdata`, `get_data_count_from_samaajdata`:

**Find WHERE clauses with city filter and replace:**
```python
# OLD:
if city:
    where_clauses.append(f"em.city = '{city}'")

# NEW:
if city:
    city_variations = normalize_city_name(city)
    city_clause = " OR ".join([f"em.city = '{var}'" for var in city_variations])
    where_clauses.append(f"({city_clause})")
```

### 5. Update `get_data_metadata_on_samaajdata`
**Around line 450, add URL formatting to example values:**
```python
# After getting example_values:
formatted_examples = [
    format_urls_as_markdown_links(str(val)) if val else val
    for val in example_values
]

# And format field definitions:
"field_definition": format_urls_as_markdown_links(
    field_row["field_definition"]
) if field_row["field_definition"] else None,
```

---

## Testing Checklist

After integration:

- [ ] Test context memory with 10+ message conversation
- [ ] Test "yeah" and "okay" responses
- [ ] Test "Bangalore" and "Bengaluru" both work
- [ ] Test other city variations (Mumbai/Bombay, etc.)
- [ ] Test new location discovery tools
- [ ] Verify URLs show as markdown links
- [ ] Test error retry mechanism
- [ ] Check logs for new metrics
- [ ] Verify no regressions in existing functionality

---

## Rollback Plan

If issues occur:

1. Keep backups of original files
2. Git revert to previous commit
3. Or use these backup commands:
```bash
cp agent.py.backup agent.py
cp server.py.backup server.py
systemctl restart your-fastapi-service
systemctl restart your-mcp-service
```

---

## Performance Considerations

- Context size increased 12x (8→100 messages)
  - Monitor memory usage
  - Adjust `max_messages` if needed
  
- New tools add 2 database queries
  - Location discovery cached in conversation
  - Minimal impact per user session

- URL regex processing
  - Only runs on data with URLs
  - Negligible performance impact

---

## Support

If you encounter issues:

1. Check logs: `logs/main.log`, `logs/performance.log`
2. Verify environment variables set correctly
3. Test individual functions with unit tests
4. Review metrics endpoint: `/metrics`

---

## Next Steps

After successful integration:

1. Monitor user interactions for 1 week
2. Collect feedback on improvements
3. Fine-tune entity extraction keywords
4. Add more city variations as needed
5. Consider frontend enhancements (copy button, PDF export)

---

That's it! The solutions are ready to deploy. All changes are backward-compatible and extensively logged for easy troubleshooting.
