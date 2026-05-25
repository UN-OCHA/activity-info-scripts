import json
import logging
import os
import sys

from dotenv import load_dotenv
from fastmcp import FastMCP
from fastmcp.server.transforms import ResourcesAsTools

from activityinfo.client import Configuration, ApiClient, DefaultApi

logging.basicConfig(level=logging.INFO, stream=sys.stderr)
logger = logging.getLogger("mcp_server")

load_dotenv()

mcp = FastMCP(
    name="ActivityInfo",
    instructions="Optimized for direct form access. Use search tools to avoid browsing large trees."
)


def get_client() -> DefaultApi:
    configuration = Configuration(
        host=os.getenv("ACTIVITYINFO_BASE_URL", "https://www.activityinfo.org/resources/"),
        access_token=os.getenv("API_TOKEN")
    )
    client = ApiClient(configuration)
    return DefaultApi(client)


# --- Optimized Tools ---

@mcp.tool()
async def search_databases(query: str) -> str:
    """
    Filters the 84+ databases by name.
    Example: 'Cameroon 2026' or 'HPC'.
    Use this to get the database_id without listing everything.
    """
    client = get_client()
    dbs = await client.get_user_databases()
    matches = [db.to_dict() for db in dbs if query.lower() in db.label.lower()]
    return json.dumps(matches, indent=2)


@mcp.tool()
async def find_form_in_database(database_id: str, form_name_query: str) -> str:
    """
    Searches for a form name within a specific database.
    Returns the form_id and parent folder.
    """
    client = get_client()
    tree = await client.get_database_tree(database_id=database_id)
    # Flatten the tree in Python to find the form
    matches = []
    # Recursive search or flat iteration depending on your model structure
    for resource in tree.resources:
        if resource.label and form_name_query.lower() in resource.label.lower():
            matches.append({"id": resource.id, "label": resource.label, "type": resource.type})

    return json.dumps(matches, indent=2)


# --- Resources (High-Density Data) ---

@mcp.tool()
async def list_databases() -> str:
    """
    List all accessible ActivityInfo databases.
    Returns a list of database names and IDs.
    Start here to find which database to explore.
    """
    client = get_client()
    dbs = await client.get_user_databases()
    return json.dumps([db.to_dict() for db in dbs], indent=2)


@mcp.resource("activityinfo://database/{database_id}")
async def get_database_structure(database_id: str) -> str:
    """Full tree for deep inspection. Use only if find_form_in_database fails."""
    client = get_client()
    tree = await client.get_database_tree(database_id)
    return tree.to_json()


@mcp.resource("activityinfo://form/{form_id}/schema")
async def get_form_schema(form_id: str) -> str:
    """Direct access to form fields and structure."""
    client = get_client()
    schema = await client.get_form_schema(form_id=form_id)
    return schema.to_json()


@mcp.resource("activityinfo://form/{form_id}/data")
async def get_form_data(form_id: str) -> str:
    """Direct access to records."""
    client = get_client()
    records = await client.get_form_records(form_id=form_id)
    return json.dumps(records, indent=2)


if __name__ == "__main__":
    mcp.add_transform(ResourcesAsTools(mcp))
    mcp.run(transport="http", host="127.0.0.1", port=6515)
