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
- Set up .env file

Since our database is hosted on RDS which needs to be accessed via a SSH tunnel, first setup the tunnel on a terminal:

```bash
ssh -i /path/to/creds.pem -N -L 54320:DB_HOST:5432 -p SSH_PORT user@EC2_INSTANCE_IP
```

```bash
DATABASE_URL=postgresql://<username>:<password>@localhost:<forwarded_port>/<database_name>
```

`forwarded_port` is the port on which the tunnel is running (i.e. 54320 in the example above).


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