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


@app.command(help="Nick's test scripts: Print Form Schema.")
def formschema(
        form_id: Annotated[str, typer.Argument(help="The ActivityInfo ID of the form")],
):
    """
    Print the form schema to console.
    """
    client = get_client()

    with handle_api_errors(f"Failed to get schema for {form_id}"):
        schema = client.api.get_form_schema(form_id)

    pprint(schema, expand_all=True)




    # for field in schema.elements:
    #     field_trans_val = get_translation(field.label)
    #     form_strings.append(DatabaseTranslation(id=f"field:{field.id}:label", original=field.label,
    #                                             translated=field_trans_val or "", autoTranslated=False))
    #     if field.description:
    #         desc_trans_val = get_translation(field.description)
    #         form_strings.append(
    #             DatabaseTranslation(id=f"field:{field.id}:description", original=field.description,
    #                                 translated=desc_trans_val or "", autoTranslated=False))
    #
    # updated_form_strings = [t for t in form_strings if t.translated]






if __name__ == "__main__":
    app()
