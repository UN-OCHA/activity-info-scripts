# import csv
from typing import Annotated, Optional

# import pandas as pd
import typer
# from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.pretty import pprint

from api.models import (
    RecordUpdateDTO,
    UpdateDatabaseDTO,
    DatabaseTreeResourceType,
    UpdateDatabaseTranslationsDTO,
    DatabaseTranslation
)
# from common import get_records_with_multiref
from utils import get_client, console, handle_api_errors

# Initialize a Typer sub-application for nick tests
app = typer.Typer(no_args_is_help=True)

@app.command(help="Nick's test scripts: Build System ID dictionary.")
def id_dictionary(
        database_id: Annotated[str, typer.Argument(help="The ActivityInfo ID of the database")],
):
    client = get_client()
    with handle_api_errors(f"Failed to get tree for {database_id}"):
        tree = client.api.get_database_tree(database_id)

    pprint(tree, expand_all=True)


@app.command(help="Nick's test scripts: Print Form Schema.")
def form_schema(
        form_id: Annotated[str, typer.Argument(help="The ActivityInfo ID of the form")],
):
    """
    Print the form schema to console.
    """
    client = get_client()

    with handle_api_errors(f"Failed to get schema for {form_id}"):
        schema = client.api.get_form_schema(form_id)

    pprint(schema, expand_all=True)

@app.command(help="Nick's test scripts: Print DB Tree.")
def db_tree(
        db_id: Annotated[str, typer.Argument(help="The ActivityInfo ID of the db")],
):
    """
    Print the form schema to console.
    """
    client = get_client()

    with handle_api_errors(f"Failed to get tree for {db_id}"):
        tree = client.api.get_database_tree(db_id)

    pprint(tree, expand_all=True)






if __name__ == "__main__":
    app()
