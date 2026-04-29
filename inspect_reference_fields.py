import asyncio
from typing import Annotated, Set, Dict

import typer
from rich.table import Table

from utils import get_client, handle_api_errors, console

app = typer.Typer(no_args_is_help=True)


def _extract_id_like_tokens(text: str) -> Set[str]:
    import re
    # Match strings that look like ActivityInfo IDs (c followed by alphanumeric)
    return set(re.findall(r"\b[a-z][a-z0-9]{5,}\b", text))


@app.command(help="Inspect a form schema for potential reference fields and IDs", no_args_is_help=True)
def inspect(
        form_id: Annotated[str, typer.Argument(help="Form ID to inspect")],
):
    asyncio.run(_inspect_async(form_id))


async def _inspect_async(form_id: str):
    client = get_client()

    with handle_api_errors(f"Could not fetch schema for form {form_id}"):
        schema = await client.get_form_schema_get(form_id)

    with handle_api_errors(f"Could not fetch database tree for {schema.database_id}"):
        tree = await client.get_database_tree_get(schema.database_id)

    forms_by_id: Dict[str, str] = {
        res.id: res.label for res in tree.resources if res.type == "FORM"
    }

    table = Table(title=f"Schema Inspection: {schema.label} ({form_id})")
    table.add_column("Field ID", style="magenta")
    table.add_column("Code", style="cyan")
    table.add_column("Type", style="green")
    table.add_column("Label", style="white")
    table.add_column("Reference / Potential IDs", style="yellow")

    for field in schema.elements:
        ref_info = "-"
        if field.type == "reference" and field.typeParameters and field.typeParameters.range:
            ref_form_id = field.typeParameters.range[0].get("formId")
            if ref_form_id:
                form_name = forms_by_id.get(ref_form_id, ref_form_id)
                ref_info = f"Ref: {form_name}"

        # Check formulas for hardcoded IDs
        formulas = []
        if field.defaultValueFormula: formulas.append(field.defaultValueFormula)
        if field.type == "calculated" and field.typeParameters and field.typeParameters.formula:
            formulas.append(field.typeParameters.formula)

        potential_ids = []
        for f in formulas:
            tokens = _extract_id_like_tokens(f)
            for t in tokens:
                if t in forms_by_id:
                    potential_ids.append(f"{t} ({forms_by_id[t]})")
                else:
                    potential_ids.append(t)

        if potential_ids:
            ref_info = f"{ref_info} | IDs: {', '.join(potential_ids)}" if ref_info != "-" else f"IDs: {', '.join(potential_ids)}"

        table.add_row(field.id, field.code or "-", field.type, field.label, ref_info)

    console.print(table)


if __name__ == "__main__":
    app()
