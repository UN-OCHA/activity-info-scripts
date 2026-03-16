import logging
from typing import Annotated

import typer

import config
import db
import forms
import translations
import users
from utils import console

# Initialize the main Typer application
# no_args_is_help=True ensures that if no command is provided, the help menu is displayed
app = typer.Typer(no_args_is_help=True)

# Register sub-applications (command groups) from different modules
# Each module's 'app' is added under a specific name and help description
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

    This is the main entry point for the ActivityInfo Scripts (AIS) CLI.
    It provides a global '--verbose' option to control logging levels across all commands.
    """
    if verbose:
        # Set the global logging level to INFO for more detailed output
        logging.basicConfig(level=logging.INFO, format="%(message)s")
        # Explicitly enable logging for the custom 'httpx_full' logger used in the API client
        logging.getLogger("httpx_full").setLevel(logging.INFO)
        console.print("[dim]Verbose logging enabled[/dim]")
    else:
        # Default to WARNING level to keep the output clean for normal use
        logging.basicConfig(level=logging.WARNING, format="%(message)s")
        # Suppress detailed HTTP logs unless verbose is requested
        logging.getLogger("httpx_full").setLevel(logging.WARNING)
        console.print("[dim]Verbose logging disabled[/dim]")


# Standard Python entry point to run the Typer application
if __name__ == "__main__":
    app()
