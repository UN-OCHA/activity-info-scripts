import typer
from rich.table import Table

from utils import get_client, handle_api_errors, console

app = typer.Typer(no_args_is_help=True)


@app.command(name="list")
def list_databases():
    """
    List all databases accessible with the current API token.
    """
    client = get_client()

    with console.status("Fetching databases...") as status:
        with handle_api_errors("Could not fetch databases"):
            databases = client.api.get_user_databases()

    if not databases:
        console.print("[yellow]No databases found.[/yellow]")
        return

    table = Table(title="Accessible Databases")
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Label", style="green")
    table.add_column("Description", style="white")

    for db in databases:
        table.add_row(db.databaseId, db.label, db.description or "")

    console.print(table)


if __name__ == "__main__":
    app()
