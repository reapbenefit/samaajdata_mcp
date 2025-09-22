from agents import GuardrailFunctionOutput
from pydantic import BaseModel, Field
import os
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
from datetime import datetime

load_dotenv()

from agents import Agent, HostedMCPTool, Runner

if os.getenv("ENV") != "development":
    root_path = "/agent"
else:
    root_path = "/"

app = FastAPI(root_path=root_path)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Chat history models
class ChatMessage(BaseModel):
    role: str  # "user" or "assistant" 
    content: str
    timestamp: Optional[datetime] = None


class QueryRequest(BaseModel):
    query: str
    chat_history: Optional[List[ChatMessage]] = []


def build_context_from_history(chat_history: List[ChatMessage], max_messages: int = 8) -> str:
    """Build context string from chat history to provide to the agent"""
    if not chat_history:
        return ""
    
    # Get recent messages (limit to avoid token overflow)
    recent_messages = chat_history[-max_messages:] if len(chat_history) > max_messages else chat_history
    
    if not recent_messages:
        return ""
    
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
    
    return "\n".join(context_parts)


async def input_guardrail(ctx, agent, input_data):
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

    result = await Runner.run(guardrail_agent, input_data, context=ctx.context)
    final_output = result.final_output_as(SamaajdataGuardrailOutput)
    return GuardrailFunctionOutput(
        output_info=final_output,
        tripwire_triggered=not final_output.is_relevant,
    )


@app.post("/respond")
async def answer_query(request: QueryRequest):
    # Build context from chat history
    context = build_context_from_history(request.chat_history)
    
    # Combine context with the current query
    enhanced_query = f"{context}{request.query}" if context else request.query
    
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
        # input_guardrails=[
        #     InputGuardrail(guardrail_function=input_guardrail),
        # ],
    )

    result = await Runner.run(agent, enhanced_query)

    return result.final_output


@app.get("/health")
async def health():
    return {"status": "ok"}
