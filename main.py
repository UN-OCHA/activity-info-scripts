import logging
from typing import Annotated

import typer

import config
import db
import forms
import translations
import users
from utils import console

app = typer.Typer(no_args_is_help=True)
app.add_typer(translations.app, name="translations", help="Migrate translations between databases")
app.add_typer(users.app, name="users", help="Manage a given database's users")
app.add_typer(forms.app, name="forms", help="Manage reference & data forms for a given database")
app.add_typer(config.app, name="config", help="Adjust metric and disaggregation fields in data forms")
app.add_typer(db.app, name="db", help="General database utilities")


@app.callback()
def main(
        verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Enable verbose logging")] = False
):
    """
    AIS CLI: A tool for managing ActivityInfo databases.
    """
    if verbose:
        logging.basicConfig(level=logging.INFO, format="%(message)s")
        # Enable logging for our specific logger in api/client.py
        logging.getLogger("httpx_full").setLevel(logging.INFO)
        console.print("[dim]Verbose logging enabled[/dim]")


if __name__ == "__main__":
    app()
