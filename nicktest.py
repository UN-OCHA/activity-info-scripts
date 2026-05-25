from typing import Annotated

import typer
from rich.pretty import pprint

from utils import get_client, handle_api_errors, wrap_async

# Initialize a Typer sub-application for nick tests
app = typer.Typer(no_args_is_help=True)


@app.command(help="Nick's test scripts: Print Form Schema.")
@wrap_async
async def print_schema(
        form_id: Annotated[str, typer.Argument(help="The ID of the form")]
):
    """
    Print the form schema to console.
    """
    client = get_client()

    with handle_api_errors(f"Failed to get schema for {form_id}"):
        schema = await client.get_form_schema(form_id=form_id)

    pprint(schema, expand_all=True)


@app.command(help="Nick's test scripts: Print DB Tree.")
@wrap_async
async def print_tree(
        db_id: Annotated[str, typer.Argument(help="The ID of the database")]
):
    """
    Print the form schema to console.
    """
    client = get_client()

    with handle_api_errors(f"Failed to get tree for {db_id}"):
        tree = await client.get_database_tree(database_id=db_id)

    pprint(tree, expand_all=True)


if __name__ == "__main__":
    app()
