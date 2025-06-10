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

## Running the server

```bash
python server.py
```

Running Inspector for debugging:

```bash
mcp dev server.py
```

## Accessing the server

```bash
http://localhost:8000/mcp/
```