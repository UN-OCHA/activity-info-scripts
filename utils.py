import os
from contextlib import contextmanager
from typing import Dict, Any

import typer
from dotenv import load_dotenv
from rich.console import Console

from activityinfo.client import Configuration, ApiClient, DefaultApi

# Load environment variables from a .env file if it exists
load_dotenv()

# Initialize a rich Console for stylized CLI output
console = Console()


def get_client() -> DefaultApi:
    configuration = Configuration(
        host=os.getenv("ACTIVITYINFO_BASE_URL", "https://www.activityinfo.org/resources/"),
        access_token=os.getenv("API_TOKEN")
    )
    client = ApiClient(configuration)
    return DefaultApi(client)


@contextmanager
def handle_api_errors(message: str):
    """
    Context manager to handle API errors and display them nicely in the console.
    """
    try:
        yield
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {message}: {e}")
        # In a CLI tool, we usually want to exit on fatal API errors
        raise typer.Exit(code=1)


def build_nested_dict(flat_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converts a flat dictionary with dot-separated keys into a nested dictionary.
    Example: {'a.b': 1, 'a.c': 2, 'd': 3} -> {'a': {'b': 1, 'c': 2}, 'd': 3}
    """
    nested: Dict[str, Any] = {}
    for key, value in flat_dict.items():
        parts = key.split(".")
        curr = nested
        for part in parts[:-1]:
            if part not in curr:
                curr[part] = {}
            curr = curr[part]
        curr[parts[-1]] = value
    return nested
