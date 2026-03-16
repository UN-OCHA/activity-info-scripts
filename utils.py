import os
from contextlib import contextmanager

import typer
from dotenv import load_dotenv
from rich.console import Console

from api import ActivityInfoClient
from api.client import APIError

# Load environment variables from a .env file if it exists
load_dotenv()

# Initialize a rich Console for stylized CLI output
console = Console()


def get_client() -> ActivityInfoClient:
    """
    Initialize and return an ActivityInfo API client.
    
    The client requires an API token, which it attempts to retrieve from the 'API_TOKEN'
    environment variable. If not found, it prompts the user for the token interactively.
    
    The base URL for the ActivityInfo API can also be configured via 'ACTIVITYINFO_BASE_URL',
    defaulting to the standard ActivityInfo production resource endpoint.
    
    Returns:
        ActivityInfoClient: An authenticated instance of the ActivityInfo client.
    """
    # Retrieve configuration from environment variables
    token = os.getenv("API_TOKEN")
    base_url = os.getenv("ACTIVITYINFO_BASE_URL", "https://www.activityinfo.org/resources/")

    # Fallback to interactive prompt if token is missing
    if token is None:
        token = typer.prompt("Please enter your ActivityInfo API token", hide_input=True)
    
    return ActivityInfoClient(base_url, token)


@contextmanager
def handle_api_errors(message: str = "An error occurred"):
    """
    A context manager to uniformly handle errors arising from API calls and other operations.
    
    This wrapper captures both expected 'APIError' exceptions from our client and 
    unexpected Python exceptions. It prints a formatted error message to the console 
    and exits the CLI with a non-zero status code.
    
    Args:
        message (str): A descriptive message to prefix the specific error output.
        
    Yields:
        None: Executes the wrapped block of code.
        
    Raises:
        typer.Exit: Terminates the CLI process with an exit code of 1 on error.
    """
    try:
        # Execute the code block within the 'with' statement
        yield
    except APIError as e:
        # Format and display errors that were explicitly raised by the API client
        console.print(f"[bold red]API Error:[/bold red] {message}")
        console.print(f"[red]{str(e)}[/red]")
        raise typer.Exit(code=1)
    except Exception as e:
        # Catch all other unexpected errors to ensure a clean exit with a helpful message
        console.print(f"[bold red]Unexpected Error:[/bold red] {message}")
        console.print(f"[red]{str(e)}[/red]")
        raise typer.Exit(code=1)
