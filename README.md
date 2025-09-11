# Samaajdata MCP Server

MCP server for connecting to data from Samaajdata.

## Setup

- Install virtual environment

```bash
python -m venv venv
```

- Activate virtual environment

```bash
source venv/bin/activate
```

- Install dependencies

```bash
pip install -r requirements.txt
```

- Copy `.env.example` to `.env` and fill in the empty values.

## Running the server

```bash
python init.py
python server.py
```

To access the server, go to http://localhost:8000/sse/

Running Inspector for debugging:

```bash
mcp dev server.py
```

## Running the agent API locally

```bash
uvicorn agent:app --reload --port 8004
```

To access the agent API docs, go to http://localhost:8004/docs

## Running the agent API on production

```bash
uvicorn agent:app --reload --port 8004 --host 0.0.0.0
```

To access the agent API docs, go to http://localhost:8004/agent/docs

## Staging

Dev MCP server: https://dev.mcp.samaajdata.org/sse
Dev Agent API: https://dev.agent.samaajdata.org/

## Production

Prod MCP server: https://mcp.samaajdata.org/sse
Prod Agent API: https://agent.samaajdata.org/
