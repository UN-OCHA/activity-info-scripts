import typer
from rich.table import Table

from utils import get_client, handle_api_errors, console

# Initialize a Typer sub-application specifically for database-level utilities
app = typer.Typer(no_args_is_help=True)


@app.command(name="list")
def list_databases():
    """
    List all databases accessible with the current API token.
    
    This command retrieves a list of all databases the authenticated user has access to 
    on the ActivityInfo platform and displays their ID, Label, and Description in 
    a formatted table.
    """
    # Instantiate the ActivityInfo API client
    client = get_client()

    # Provide a visual status indicator while the API request is in progress
    with console.status("Fetching databases...") as status:
        # Wrap the call in our standard error handler to catch and display any issues
        with handle_api_errors("Could not fetch databases"):
            # Call the API to get the user's accessible databases
            databases = client.api.get_user_databases()

    # Check if the result set is empty and inform the user
    if not databases:
        console.print("[yellow]No databases found.[/yellow]")
        return

    # Initialize a rich Table for stylized output of the database list
    table = Table(title="Accessible Databases")
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Label", style="green")
    table.add_column("Description", style="white")

    # Iterate through the database objects and add them to the table
    for db in databases:
        # Ensure that missing descriptions are handled gracefully as empty strings
        table.add_row(db.databaseId, db.label, db.description or "")

    # Output the final table to the terminal
    console.print(table)


# Standard Python entry point for local testing or direct execution of this submodule
if __name__ == "__main__":
    app()
