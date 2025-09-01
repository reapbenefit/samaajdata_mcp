from agents import Agent, InputGuardrail, GuardrailFunctionOutput, Runner
from pydantic import BaseModel, Field
import os
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

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


class QueryRequest(BaseModel):
    query: str


@app.post("/respond")
async def answer_query(request: QueryRequest):
    agent = Agent(
        name="Samaajdata Assistant",
        instructions="You are a helpful assistant that can answer questions about samaajdata using the tools provided. It is possible that the user's query is very vague and you need to use the tools in multiple steps to answer the question. Try your absolute best to answer the question even if it does not have a lot of details by repeatedly using the appropriate tools out of the ones provided, reflecting on the outputs of the previous steps and analysing if using the tools again can help you answer the question. The user would highly prefer a visual representation of the data and if you can provide one using the tools provided, do so even if the user hasn't explicitly asked for one in their query. If none of the tools can be used to make the data more visually appealing or readable, use markdown formatting to appropriately format the data as if it can be used in a report analysing the data (e.g. using heading, lists, tables, bold, colors etc.). If you are not sure about the answer, you can say so and ask the user to provide more details. But do this only after you have exhaustively explored all the possibilities through the tools provided.",
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

    result = await Runner.run(agent, request.query)

    return result.final_output


@app.get("/health")
async def health():
    return {"status": "ok"}
