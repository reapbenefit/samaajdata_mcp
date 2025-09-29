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

@log_execution_time("build_context_from_history", main_logger)
def build_context_from_history(chat_history: List[ChatMessage], max_messages: int = 8) -> str:
    """Build context string from chat history to provide to the agent"""
    
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
    
    context_parts = [
        "=== CONVERSATION CONTEXT ===",
        "Here is the recent conversation history to help you provide contextual responses:",
        ""
    ]
    
    for msg in recent_messages:
        role_prefix = "User" if msg.role == "user" else "Assistant"
        context_parts.append(f"{role_prefix}: {msg.content}")
    
    context_parts.extend([
        "",
        "=== END CONTEXT ===",
        "",
        "Based on this conversation context, please respond to the following new query. Reference previous messages when relevant and maintain conversation continuity:",
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
    """Main endpoint to answer user queries with comprehensive logging"""
    
    main_logger.info(f"Processing query request: {len(request.query)} characters, "
                    f"{len(request.chat_history)} history messages")
    
    async with log_async_operation("query_processing", perf_logger, include_memory=True):
        
        # Build context from chat history
        async with log_async_operation("context_building", main_logger):
            context = build_context_from_history(request.chat_history)
            
            # Combine context with the current query
            enhanced_query = f"{context}{request.query}" if context else request.query
            
            enhanced_query_length = len(enhanced_query)
            main_logger.info(f"Enhanced query prepared: {enhanced_query_length} characters "
                            f"(original: {len(request.query)}, context: {len(context)})")

        # Enhanced instructions that account for conversation context
        instructions = """You are a helpful assistant that can answer questions about samaajdata using the tools provided. 

        IMPORTANT CONTEXT HANDLING:
        - If conversation context is provided above, use it to maintain continuity and provide more relevant responses
        - Reference previous topics, data requests, or visualizations when appropriate
        - Avoid repeating information already provided unless specifically asked
        - Build upon previous analysis or extend earlier findings when relevant

        ANALYSIS APPROACH:
        It is possible that the user's query is very vague and you need to use the tools in multiple steps to answer the question. Try your absolute best to answer the question even if it does not have a lot of details by repeatedly using the appropriate tools out of the ones provided, reflecting on the outputs of the previous steps and analysing if using the tools again can help you answer the question. 

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
            
            return final_output

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