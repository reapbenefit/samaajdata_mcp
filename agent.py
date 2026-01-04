import asyncio
from agents import GuardrailFunctionOutput
from pydantic import BaseModel, Field
import os
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
from datetime import datetime
import uuid
import time
import re
import json
from pathlib import Path

# Import logging functionality
from logging_config import (
    main_logger, 
    perf_logger,
    log_execution_time, 
    log_async_operation,
    request_tracker,
    performance_metrics
)

load_dotenv()

from agents import Agent, HostedMCPTool, Runner

if os.getenv("ENV") != "development":
    root_path = "/agent"
else:
    root_path = "/"

app = FastAPI(root_path=root_path)

# Initialize logging
main_logger.info(f"Starting FastAPI application with root_path: {root_path}")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

main_logger.info("CORS middleware configured for all origins")

# Solution keywords mapping - loaded from file
SOLUTION_KEYWORDS_FILE = Path(__file__).parent / "solution_keywords.json"

@log_execution_time("load_solution_keywords", main_logger)
def load_solution_keywords() -> List[dict]:
    """
    Load solution keywords from JSON file.
    Returns a list of dictionaries with 'keywords' and 'solution' keys.
    """
    try:
        if not SOLUTION_KEYWORDS_FILE.exists():
            main_logger.warning(f"Solution keywords file not found: {SOLUTION_KEYWORDS_FILE}. Using empty list.")
            return []
        
        with open(SOLUTION_KEYWORDS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Validate structure
        if not isinstance(data, list):
            main_logger.error(f"Invalid format in {SOLUTION_KEYWORDS_FILE}: expected list, got {type(data)}")
            return []
        
        # Validate each entry has required keys
        valid_data = []
        for i, entry in enumerate(data):
            if not isinstance(entry, dict):
                main_logger.warning(f"Skipping invalid entry at index {i}: expected dict, got {type(entry)}")
                continue
            if "keywords" not in entry or "solution" not in entry:
                main_logger.warning(f"Skipping invalid entry at index {i}: missing 'keywords' or 'solution' key")
                continue
            valid_data.append(entry)
        
        main_logger.info(f"Loaded {len(valid_data)} solution keyword entries from {SOLUTION_KEYWORDS_FILE}")
        return valid_data
        
    except json.JSONDecodeError as e:
        main_logger.error(f"Failed to parse JSON from {SOLUTION_KEYWORDS_FILE}: {e}")
        return []
    except Exception as e:
        main_logger.error(f"Error loading solution keywords from {SOLUTION_KEYWORDS_FILE}: {e}")
        return []

# Load solution keywords at module level
SOLUTION_KEYWORDS = load_solution_keywords()

class SolutionMatchOutput(BaseModel):
    """Output model for solution matching AI call"""
    relevant_solutions: List[str] = Field(
        description="List of relevant solutions that match the user's query"
    )
    reasoning: str = Field(
        description="Brief explanation of why these solutions are relevant"
    )

@log_execution_time("find_relevant_solutions", main_logger)
async def find_relevant_solutions(query: str) -> List[str]:
    """
    Use AI to find relevant solutions based on the query.
    Analyzes the query against available solutions and returns the most relevant ones.
    """
    if not SOLUTION_KEYWORDS:
        main_logger.warning("No solution keywords loaded, returning empty list")
        return []
    
    # Build the available solutions context for the AI
    solutions_context = "Available solutions:\n\n"
    for i, entry in enumerate(SOLUTION_KEYWORDS, 1):
        keywords_str = ", ".join(entry["keywords"])
        solutions_context += f"{i}. Keywords: {keywords_str}\n"
        solutions_context += f"   Solution: {entry['solution']}\n\n"
    
    # Create instructions for the solution matching agent
    solution_matching_instructions = f"""You are a solution matching assistant. Your task is to analyze a user's query and identify which solutions from the available list are relevant.

{solutions_context}

Analyze the user's query and determine which solutions are relevant. A solution is relevant if:
- The query mentions topics, issues, or keywords related to the solution
- The query asks about problems that the solution addresses
- The query seeks help or information about topics covered by the solution

Return only the solutions that are genuinely relevant to the user's query. Do not include solutions that are only tangentially related."""

    # Create the solution matching agent
    solution_agent = Agent(
        name="Solution Matcher",
        instructions=solution_matching_instructions,
        model="gpt-4.1",
        output_type=SolutionMatchOutput,
    )
    
    try:
        main_logger.debug("Running AI-based solution matching...")
        result = await Runner.run(solution_agent, query)
        output = result.final_output_as(SolutionMatchOutput)
        
        main_logger.info(f"AI found {len(output.relevant_solutions)} relevant solutions. Reasoning: {output.reasoning[:100]}...")
        return output.relevant_solutions
        
    except Exception as e:
        main_logger.error(f"Error in AI-based solution matching: {e}")
        # Fallback to empty list if AI call fails
        return []

# Chat history models
class ChatMessage(BaseModel):
    role: str  # "user" or "assistant" 
    content: str
    timestamp: Optional[datetime] = None

class QueryRequest(BaseModel):
    query: str
    chat_history: Optional[List[ChatMessage]] = []

# Middleware for request tracking
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Middleware to log all HTTP requests and their processing times"""
    
    # Generate unique request ID
    request_id = str(uuid.uuid4())[:8]
    
    # Extract basic request info
    method = request.method
    url = str(request.url)
    endpoint = request.url.path
    
    # Log request start
    start_time = time.time()
    main_logger.info(f"Request started: {request_id} | {method} {endpoint}")
    
    # Start tracking this request
    request_tracker.start_request(request_id, endpoint)
    
    try:
        # Process the request
        response = await call_next(request)
        
        # Calculate processing time
        process_time = time.time() - start_time
        
        # Log request completion
        main_logger.info(f"Request completed: {request_id} | {method} {endpoint} | "
                        f"Status: {response.status_code} | Time: {process_time*1000:.2f}ms")
        
        # End tracking this request
        request_tracker.end_request(request_id, response.status_code)
        
        # Add custom headers for debugging
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Process-Time"] = f"{process_time*1000:.2f}ms"
        
        # Record metrics based on endpoint
        performance_metrics.record_metric(f"endpoint_{endpoint.replace('/', '_')}", process_time * 1000, "ms")
        
        return response
        
    except Exception as e:
        # Calculate processing time even for errors
        process_time = time.time() - start_time
        
        # Log request error
        main_logger.error(f"Request failed: {request_id} | {method} {endpoint} | "
                         f"Error: {str(e)} | Time: {process_time*1000:.2f}ms")
        
        # End tracking this request with error status
        request_tracker.end_request(request_id, 500)
        
        # Record error metrics
        performance_metrics.record_metric("request_errors", 1, "count")
        
        raise

@log_execution_time("extract_entities_from_history", main_logger)
def extract_entities_from_history(chat_history: List[ChatMessage]) -> dict:
    """
    Extract key entities (locations, categories, numbers) from chat history for better context
    """
    entities = {
        "locations": set(),
        "categories": set(),
        "numbers": [],
        "partners": set(),
        "topics": set()
    }
    
    # Common location patterns
    location_keywords = ["bangalore", "bengaluru", "delhi", "mumbai", "chennai", "hyderabad", 
                        "kolkata", "pune", "ahmedabad", "jp nagar", "koramangala", "indiranagar"]
    
    # Common category keywords
    category_keywords = ["waste", "garbage", "sanitation", "water", "flooding", "street lights",
                        "public toilets", "parks", "trees", "schemes", "corruption", "bribes"]
    
    # Common partner names
    partner_keywords = ["gram vaani", "reap benefit", "open city", "video volunteers", 
                       "paani earth", "gram chetna"]
    
    for msg in chat_history:
        content_lower = msg.content.lower()
        
        # Extract locations
        for loc in location_keywords:
            if loc in content_lower:
                entities["locations"].add(loc.title())
        
        # Extract categories
        for cat in category_keywords:
            if cat in content_lower:
                entities["categories"].add(cat.title())
        
        # Extract partners
        for partner in partner_keywords:
            if partner in content_lower:
                entities["partners"].add(partner.title())
        
        # Extract numbers (for counts, dates, etc.)
        numbers = re.findall(r'\b\d+(?:,\d+)*(?:\.\d+)?\b', msg.content)
        entities["numbers"].extend(numbers)
    
    # Convert sets to lists for JSON serialization
    entities["locations"] = list(entities["locations"])
    entities["categories"] = list(entities["categories"])
    entities["partners"] = list(entities["partners"])
    
    main_logger.debug(f"Extracted entities: {entities}")
    
    return entities

@log_execution_time("resolve_contextual_references", main_logger)
def resolve_contextual_references(query: str, chat_history: List[ChatMessage], entities: dict) -> str:
    """
    Resolve vague references like 'it', 'that', 'this', 'yeah', 'okay' using context
    """
    query_lower = query.lower().strip()
    
    # Handle short confirmations
    short_confirmations = ["yeah", "yes", "okay", "ok", "sure", "yep", "right", "correct"]
    
    if query_lower in short_confirmations and len(chat_history) >= 2:
        # Get the last assistant message
        last_assistant = None
        for msg in reversed(chat_history):
            if msg.role == "assistant":
                last_assistant = msg.content
                break
        
        if last_assistant:
            # Check if assistant offered options or asked a question
            offer_patterns = [
                r"would you like to see (.+?)\?",
                r"would you like (.+?)\?",
                r"let me know if you want (.+?)[.!?]",
                r"if you (?:want|need) (.+?)[,.]",
                r"do you want (.+?)\?",
                r"shall I (.+?)\?"
            ]
            
            for pattern in offer_patterns:
                match = re.search(pattern, last_assistant, re.IGNORECASE)
                if match:
                    offered_action = match.group(1).strip()
                    resolved = f"Yes, please {offered_action}"
                    main_logger.info(f"Resolved '{query}' to '{resolved}' based on offer")
                    return resolved
    
    # Handle contextual references with entities
    if query_lower in ["more details", "tell me more", "details", "more info", "elaborate"]:
        if len(chat_history) >= 1:
            # Get the last user query
            last_user_query = None
            for msg in reversed(chat_history):
                if msg.role == "user":
                    last_user_query = msg.content
                    break
            
            if last_user_query:
                resolved = f"Can you provide more detailed information about: {last_user_query}"
                main_logger.info(f"Resolved '{query}' with context from previous query")
                return resolved
    
    # Handle location references
    if re.search(r'\b(there|that place|this area|this city|this location)\b', query_lower):
        if entities["locations"]:
            location = entities["locations"][-1]  # Most recent location
            query = re.sub(r'\b(there|that place|this area|this city|this location)\b', 
                          location, query, flags=re.IGNORECASE)
            main_logger.info(f"Resolved location reference to '{location}'")
    
    # Handle category/topic references
    if re.search(r'\b(that|this|it)\b', query_lower) and len(query.split()) < 5:
        if entities["categories"]:
            category = entities["categories"][-1]  # Most recent category
            query = f"{query} related to {category}"
            main_logger.info(f"Added category context: {category}")
    
    return query

@log_execution_time("build_context_from_history", main_logger)
def build_context_from_history(chat_history: List[ChatMessage], max_messages: int = 100) -> str:
    """Build context string from chat history to provide to the agent (increased from 8 to 100)"""
    
    main_logger.debug(f"Building context from {len(chat_history)} messages (max: {max_messages})")
    
    if not chat_history:
        main_logger.debug("No chat history provided")
        return ""
    
    # Get recent messages (limit to avoid token overflow)
    recent_messages = chat_history[-max_messages:] if len(chat_history) > max_messages else chat_history
    
    if not recent_messages:
        main_logger.debug("No recent messages found")
        return ""
    
    main_logger.info(f"Using {len(recent_messages)} recent messages for context")
    
    # Extract entities for better context understanding
    entities = extract_entities_from_history(recent_messages)
    
    context_parts = [
        "=== CONVERSATION CONTEXT ===",
        "Here is the recent conversation history to help you provide contextual responses:",
        ""
    ]
    
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
    
    for msg in recent_messages:
        role_prefix = "User" if msg.role == "user" else "Assistant"
        context_parts.append(f"{role_prefix}: {msg.content}")
    
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
    
    context_string = "\n".join(context_parts)
    context_length = len(context_string)
    
    main_logger.debug(f"Built context string: {context_length} characters")
    
    # Record context metrics
    performance_metrics.record_metric("context_length", context_length, "chars")
    performance_metrics.record_metric("context_messages", len(recent_messages), "count")
    
    return context_string

@log_execution_time("input_guardrail", perf_logger)
async def input_guardrail(ctx, agent, input_data):
    """Input guardrail to check if query is relevant to samaajdata"""
    
    main_logger.info("Running input guardrail check")
    
    async with log_async_operation("guardrail_processing", perf_logger):
        class SamaajdataGuardrailOutput(BaseModel):
            is_relevant: bool = Field(
                description="Whether the user's query is related to data hosted on samaajdata"
            )
            reasoning: str

        guardrail_agent = Agent(
            name="Guardrail check",
            instructions="Check if the user's query is related to data hosted on samaajdata",
            output_type=SamaajdataGuardrailOutput,
            tools=[
                HostedMCPTool(
                    tool_config={
                        "type": "mcp",
                        "server_label": "Samaajdata",
                        "server_url": os.getenv("SAMAAJDATA_MCP_URL"),
                        "require_approval": "never",
                    }
                )
            ],
        )

        main_logger.debug("Guardrail agent created, running check...")
        
        result = await Runner.run(guardrail_agent, input_data, context=ctx.context)
        final_output = result.final_output_as(SamaajdataGuardrailOutput)
        
        main_logger.info(f"Guardrail check completed: relevant={final_output.is_relevant}, "
                        f"reasoning={final_output.reasoning[:100]}...")
        
        # Record guardrail metrics
        performance_metrics.record_metric("guardrail_checks", 1, "count")
        if final_output.is_relevant:
            performance_metrics.record_metric("guardrail_relevant", 1, "count")
        else:
            performance_metrics.record_metric("guardrail_irrelevant", 1, "count")
        
        return GuardrailFunctionOutput(
            output_info=final_output,
            tripwire_triggered=not final_output.is_relevant,
        )

@app.post("/respond")
@log_execution_time("answer_query_endpoint", perf_logger)
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
                
                # Extract entities from chat history
                entities = extract_entities_from_history(request.chat_history) if request.chat_history else {}
                
                # Resolve contextual references in the query
                resolved_query = resolve_contextual_references(
                    request.query, 
                    request.chat_history,
                    entities
                )
                
                if resolved_query != request.query:
                    main_logger.info(f"Query resolved from '{request.query}' to '{resolved_query}'")
                
                # Find relevant solutions using AI analysis
                async with log_async_operation("solution_finding", main_logger):
                    relevant_solutions = await find_relevant_solutions(resolved_query)
                
                # Build context from chat history
                async with log_async_operation("context_building", main_logger):
                    context = build_context_from_history(request.chat_history)
                    
                    # Combine context with the resolved query
                    enhanced_query = f"{context}{resolved_query}" if context else resolved_query
                    
                    enhanced_query_length = len(enhanced_query)
                    main_logger.info(f"Enhanced query prepared: {enhanced_query_length} characters "
                                    f"(original: {len(request.query)}, context: {len(context)})")

                # Build solutions section for instructions
                solutions_section = ""
                if relevant_solutions:
                    solutions_section = "\n\n                SOLUTIONS TO INCLUDE IN RESPONSE:\n"
                    solutions_section += "                After using the MCPTool to gather data and information, you MUST also include relevant solutions in your response.\n"
                    solutions_section += "                The following solutions have been identified as relevant to the user's query through AI analysis:\n\n"
                    for i, solution in enumerate(relevant_solutions, 1):
                        solutions_section += f"                Solution {i}: {solution}\n\n"
                    solutions_section += "                IMPORTANT: Integrate these solutions naturally into your response after presenting the data from the tools. "
                    solutions_section += "The solutions should help the user take action or understand next steps related to their query."

                # Enhanced instructions that account for conversation context
                instructions = f"""You are a helpful assistant that can answer questions about samaajdata using the tools provided.{solutions_section} 

                IMPORTANT CONTEXT HANDLING:
                - ALWAYS read and understand the conversation context provided above carefully
                - Use context to maintain continuity and provide more relevant responses
                - Reference previous topics, data requests, or visualizations when appropriate
                - Avoid repeating information already provided unless specifically asked
                - Build upon previous analysis or extend earlier findings when relevant
                - When user says "yeah", "okay", or similar short confirmations, they are accepting offers you made
                - Resolve pronoun references like "it", "that", "this" using conversation context

                CONTEXTUAL REFERENCE RESOLUTION:
                - If the query mentions "it", "that", "this" without context, check previous messages
                - If user says "more details" or "tell me more", expand on the last topic discussed
                - If location/category is implied but not stated, use the most recent one from context
                - Track numerical values mentioned (counts, percentages) for later reference

                ANALYSIS APPROACH:
                It is possible that the user's query is very vague and you need to use the tools in multiple steps to answer the question. Try your absolute best to answer the question even if it does not have a lot of details by repeatedly using the appropriate tools out of the ones provided, reflecting on the outputs of the previous steps and analysing if using the tools again can help you answer the question.
                
                WORKFLOW FOR RESPONSES:
                1. First, use the MCPTool to gather relevant data and information about the user's query
                2. Analyze the data retrieved from the tools
                3. If solutions are provided above (in the SOLUTIONS section), integrate them into your response after presenting the data
                4. Present the data first, then provide actionable solutions or next steps
                5. Make sure solutions are relevant and naturally flow with the data presentation 
                6. Add the solution in a different paragraph after the data presentation. You may use markdown formatting to appropriately format the solution as if it can be used in a report analysing the data (e.g. using heading, lists, tables, bold, colors etc.).
                7. Also use a relevant heading after the data presentation to highlight the solutions.

                LOCATION DISCOVERY:
                - When a user asks about data for a location, FIRST check if that location has data available
                - If the requested location has no data, suggest alternative nearby locations or broader regions
                - Always inform users about available locations when they ask about a place with no data

                VISUALIZATION PREFERENCE:
                The user would highly prefer a visual representation of the data and if you can provide one using the tools provided, do so even if the user hasn't explicitly asked for one in their query. If none of the tools can be used to make the data more visually appealing or readable, use markdown formatting to appropriately format the data as if it can be used in a report analysing the data (e.g. using heading, lists, tables, bold, colors etc.). 

                FALLBACK:
                If you are not sure about the answer, you can say so and ask the user to provide more details. But do this only after you have exhaustively explored all the possibilities through the tools provided."""

                # Create agent with tools
                async with log_async_operation("agent_creation", main_logger):
                    agent = Agent(
                        name="Samaajdata Assistant",
                        instructions=instructions,
                        model="gpt-4.1",
                        tools=[
                            HostedMCPTool(
                                tool_config={
                                    "type": "mcp",
                                    "server_label": "Samaajdata",
                                    "server_url": os.getenv("SAMAAJDATA_MCP_URL"),
                                    "require_approval": "never",
                                }
                            )
                        ],
                        # Note: input_guardrails are commented out for now
                        # input_guardrails=[
                        #     InputGuardrail(guardrail_function=input_guardrail),
                        # ],
                    )
                    
                    main_logger.debug("Samaajdata assistant agent created successfully")

                # Run agent processing
                async with log_async_operation("agent_execution", perf_logger):
                    main_logger.info("Starting agent execution...")
                    
                    result = await Runner.run(agent, enhanced_query)
                    
                    main_logger.info("Agent execution completed successfully")
                    
                    # Extract result
                    final_output = result.final_output
                    
                    # Log result statistics
                    if isinstance(final_output, str):
                        result_length = len(final_output)
                        main_logger.info(f"Agent result: {result_length} characters")
                        performance_metrics.record_metric("response_length", result_length, "chars")
                    
                    # Record successful query metrics
                    performance_metrics.record_metric("successful_queries", 1, "count")
                    if retry_count > 0:
                        performance_metrics.record_metric("successful_retries", 1, "count")
                    
                    return final_output
                    
        except Exception as e:
            last_error = e
            retry_count += 1
            
            main_logger.error(f"Query processing attempt {retry_count} failed: {str(e)}")
            
            if retry_count <= max_retries:
                main_logger.info(f"Retrying query (attempt {retry_count + 1} of {max_retries + 1})...")
                performance_metrics.record_metric("query_retries", 1, "count")
                # Small delay before retry
                await asyncio.sleep(0.5 * retry_count)
            else:
                # All retries exhausted
                performance_metrics.record_metric("query_failures", 1, "count")
                main_logger.error(f"All retry attempts exhausted for query")
                
                # Return user-friendly error message
                error_message = """I apologize, but I encountered an error while processing your request. 

                Here are some suggestions:
                1. Try rephrasing your query with more specific details
                2. If asking about a location, ensure you're using the correct name (e.g., 'Bengaluru' instead of 'Bangalore')
                3. If this is a follow-up question, try including more context from your previous question
                4. Try asking about a different dataset or topic

                If the issue persists, please try again in a moment."""
                
                return error_message

@app.get("/health")
@log_execution_time("health_check", main_logger)
async def health():
    """Health check endpoint with logging"""
    main_logger.debug("Health check requested")
    
    # Record health check metrics
    performance_metrics.record_metric("health_checks", 1, "count")
    
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

@app.get("/metrics")
@log_execution_time("metrics_endpoint", main_logger)
async def get_metrics():
    """Endpoint to retrieve performance metrics"""
    main_logger.info("Metrics requested")
    
    # Log current metrics summary
    performance_metrics.log_summary()
    
    return {
        "status": "ok",
        "message": "Metrics logged to performance.log file",
        "timestamp": datetime.now().isoformat()
    }

# Startup event
@app.on_event("startup")
async def startup_event():
    """Application startup logging"""
    main_logger.info("=== APPLICATION STARTUP ===")
    main_logger.info(f"Environment: {os.getenv('ENV', 'development')}")
    main_logger.info(f"Root path: {root_path}")
    main_logger.info(f"SAMAAJDATA_MCP_URL: {os.getenv('SAMAAJDATA_MCP_URL', 'Not set')}")
    main_logger.info("Application startup completed")

# Shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    """Application shutdown logging"""
    main_logger.info("=== APPLICATION SHUTDOWN ===")
    
    # Log final metrics summary
    performance_metrics.log_summary()
    
    main_logger.info("Application shutdown completed")

if __name__ == "__main__":
    main_logger.info("Starting application directly (not via server)")