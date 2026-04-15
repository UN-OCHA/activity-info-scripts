from typing import Dict, List, Set

import typer
from rich.table import Table

from api.models import FieldType
from id_translation import SchemaIdTranslator
from utils import get_client, handle_api_errors, console

app = typer.Typer(no_args_is_help=True)


def _extract_id_like_tokens(text: str) -> Set[str]:
    return SchemaIdTranslator.extract_id_like_tokens(text)


@app.command(help="Inspect reference-field behavior and ID usage for a form schema")
def inspect(
    form_id: str = typer.Argument(..., help="Form ID to inspect"),
):
    client = get_client()

    with handle_api_errors(f"Could not fetch schema for form {form_id}"):
        schema = client.api.get_form_schema(form_id)

    with handle_api_errors(f"Could not fetch database tree for {schema.databaseId}"):
        tree = client.api.get_database_tree(schema.databaseId)

    forms_by_id: Dict[str, str] = {
        res.id: res.label for res in tree.resources if res.type == "FORM"
    }
    fields_by_id: Dict[str, dict] = {
        e.id: {"code": e.code, "label": e.label, "type": e.type}
        for e in schema.elements
    }

    console.print(
        f"[bold cyan]Inspecting form[/bold cyan] {schema.label} ({schema.id}) in database {schema.databaseId}"
    )

    ref_table = Table(title="Reference Field Wiring")
    ref_table.add_column("Field")
    ref_table.add_column("Field ID")
    ref_table.add_column("Range Targets (formId -> label)")
    ref_table.add_column("Lookup Formulas")
    ref_table.add_column("ID-like Tokens In Lookups")

    ref_fields = [
        e for e in schema.elements if e.type in {FieldType.reference, FieldType.reversereference, "reference", "reversereference"}
    ]

    if not ref_fields:
        console.print("[yellow]No reference/reverse-reference fields found.[/yellow]")
    else:
        for field in ref_fields:
            tp = field.type_parameters
            range_lines: List[str] = []
            lookup_lines: List[str] = []
            token_lines: List[str] = []

            for rng in (tp.range if tp and tp.range else []):
                target_id = rng.get("formId")
                target_label = forms_by_id.get(target_id, "<not found in this DB>")
                range_lines.append(f"{target_id} -> {target_label}")

            for lookup in (tp.lookup_configs if tp and tp.lookup_configs else []):
                formula = lookup.formula or ""
                lookup_lines.append(f"{lookup.lookupLabel or '-'}: {formula}")
                tokens = sorted(_extract_id_like_tokens(formula))
                if tokens:
                    decoded = []
                    for token in tokens:
                        if token in forms_by_id:
                            decoded.append(f"{token} (FORM:{forms_by_id[token]})")
                        elif token in fields_by_id:
                            f_meta = fields_by_id[token]
                            decoded.append(f"{token} (FIELD:{f_meta['code']})")
                        else:
                            decoded.append(f"{token} (?)")
                    token_lines.append(", ".join(decoded))

            ref_table.add_row(
                f"{field.code} ({field.label})",
                field.id,
                "\n".join(range_lines) if range_lines else "-",
                "\n".join(lookup_lines) if lookup_lines else "-",
                "\n".join(token_lines) if token_lines else "-",
            )

        console.print(ref_table)

    formula_table = Table(title="Formula Fields And ID-like Tokens")
    formula_table.add_column("Field")
    formula_table.add_column("Property")
    formula_table.add_column("Formula")
    formula_table.add_column("Tokens Decoded")

    formula_rows = 0
    for field in schema.elements:
        candidates = {
            "defaultValueFormula": field.default_value_formula,
            "validationCondition": field.validation_condition,
            "relevanceCondition": field.relevance_condition,
            "typeParameters.formula": field.type_parameters.formula if field.type_parameters else None,
            "typeParameters.prefixFormula": field.type_parameters.prefix_formula if field.type_parameters else None,
        }

        for prop, formula in candidates.items():
            if not formula:
                continue
            tokens = sorted(_extract_id_like_tokens(formula))
            decoded = []
            for token in tokens:
                if token in forms_by_id:
                    decoded.append(f"{token} (FORM:{forms_by_id[token]})")
                elif token in fields_by_id:
                    f_meta = fields_by_id[token]
                    decoded.append(f"{token} (FIELD:{f_meta['code']})")
                else:
                    decoded.append(f"{token} (?)")

            formula_table.add_row(
                f"{field.code} ({field.label})",
                prop,
                formula,
                ", ".join(decoded) if decoded else "-",
            )
            formula_rows += 1

    if formula_rows:
        console.print(formula_table)
    else:
        console.print("[yellow]No formula-bearing properties found.[/yellow]")


if __name__ == "__main__":
    app()
