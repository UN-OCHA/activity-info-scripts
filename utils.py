import os
from contextlib import contextmanager

import typer
from rich.console import Console
from dotenv import load_dotenv

from api import ActivityInfoClient
from api.client import APIError

load_dotenv()

console = Console()


def get_client():
    token = os.getenv("API_TOKEN")
    base_url = os.getenv("ACTIVITYINFO_BASE_URL", "https://www.activityinfo.org/resources/")
    if token is None:
        token = typer.prompt("Please enter your ActivityInfo API token", hide_input=True)
    return ActivityInfoClient(base_url, token)


@contextmanager
def handle_api_errors(message: str = "An error occurred"):
    try:
        yield
    except APIError as e:
        console.print(f"[bold red]API Error:[/bold red] {message}")
        console.print(f"[red]{str(e)}[/red]")
        raise typer.Exit(code=1)
    except Exception as e:
        console.print(f"[bold red]Unexpected Error:[/bold red] {message}")
        console.print(f"[red]{str(e)}[/red]")
        raise typer.Exit(code=1)