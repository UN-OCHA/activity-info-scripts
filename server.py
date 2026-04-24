import json
import logging
import os
import sys

from dotenv import load_dotenv
from fastmcp import FastMCP
from fastmcp.server.transforms import ResourcesAsTools

from api import ActivityInfoClient

logging.basicConfig(level=logging.INFO, stream=sys.stderr)
logger = logging.getLogger("mcp_server")

load_dotenv()

mcp = FastMCP(
    name="ActivityInfo",
    instructions="Optimized for direct form access. Use search tools to avoid browsing large trees."
)


def get_client() -> ActivityInfoClient:
    token = os.getenv("API_TOKEN")
    if not token:
        raise ValueError("API_TOKEN missing.")
    return ActivityInfoClient(os.getenv("ACTIVITYINFO_BASE_URL", "https://www.activityinfo.org/resources/"), token)


# --- Optimized Tools ---

@mcp.tool()
def search_databases(query: str) -> str:
    """
    Filters the 84+ databases by name.
    Example: 'Cameroon 2026' or 'HPC'.
    Use this to get the database_id without listing everything.
    """
    with get_client() as client:
        dbs = client.api.get_user_databases()
        matches = [db.model_dump() for db in dbs if query.lower() in db.label.lower()]
        return json.dumps(matches, indent=2)


@mcp.tool()
def find_form_in_database(database_id: str, form_name_query: str) -> str:
    """
    Searches for a form name within a specific database.
    Returns the form_id and parent folder.
    """
    with get_client() as client:
        tree = client.api.get_database_tree(database_id)
        # Flatten the tree in Python to find the form
        matches = []
        # Recursive search or flat iteration depending on your model structure
        for resource in tree.resources:
            if form_name_query.lower() in resource.label.lower():
                matches.append({"id": resource.id, "label": resource.label, "type": resource.type})

        return json.dumps(matches, indent=2)


# --- Resources (High-Density Data) ---

@mcp.tool()
def list_databases() -> str:
    """
    List all accessible ActivityInfo databases.
    Returns a list of database names and IDs.
    Start here to find which database to explore.
    """
    with get_client() as client:
        dbs = client.api.get_user_databases()
        return json.dumps([db.model_dump() for db in dbs], indent=2)


@mcp.resource("activityinfo://database/{database_id}")
def get_database_structure(database_id: str) -> str:
    """Full tree for deep inspection. Use only if find_form_in_database fails."""
    with get_client() as client:
        return client.api.get_database_tree(database_id).model_dump_json(indent=2)


@mcp.resource("activityinfo://form/{form_id}/schema")
def get_form_schema(form_id: str) -> str:
    """Direct access to form fields and structure."""
    with get_client() as client:
        return client.api.get_form_schema(form_id).model_dump_json(indent=2)


@mcp.resource("activityinfo://form/{form_id}/data")
def get_form_data(form_id: str) -> str:
    """Direct access to records."""
    with get_client() as client:
        return json.dumps(client.api.get_form(form_id), indent=2)


if __name__ == "__main__":
    mcp.add_transform(ResourcesAsTools(mcp))
    mcp.run(transport="http", host="127.0.0.1", port=6515)
