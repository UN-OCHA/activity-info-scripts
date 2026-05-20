# import csv
import json
import os
import re
from typing import Annotated, Optional, List

import jsonpatch
# import pandas as pd
import typer
# from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.pretty import pprint
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from api.models import (
    RecordUpdateDTO,
    UpdateDatabaseDTO,
    DatabaseTreeResourceType,
    UpdateDatabaseTranslationsDTO,
    DatabaseTranslation, FormSchema
)
from id_translation import SchemaIdTranslator
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
    Print the database tree to console.
    """
    client = get_client()

    with handle_api_errors(f"Failed to get tree for {db_id}"):
        tree = client.api.get_database_tree(db_id)

    pprint(tree, expand_all=True)


@app.command(help="Apply multiple JSON patches to forms in one or more target databases", no_args_is_help=True)
def apply(
        target_database_ids: Annotated[List[str], typer.Argument(help="The list of target database IDs")],
        multipatch_file: Annotated[
            str, typer.Option("--patch", "-p", help="Path to the JSON patch file")] = "form_multipatch.json",
        dry_run: Annotated[bool, typer.Option(help="Do not actually perform any changes")] = False,
        yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation prompt")] = False,
):
    """
    Apply a series of semantic JSON patches to one or more forms in one or more databases.

    This command:
    1. Loads the multipatch file .
    2. Identifies the forms by label across databases.
    3. For each form, fetches the current schema for that form in each target database.
    4. Converts the schema's 'elements' to a code-keyed structure for patching.
    5. Applies the patch and translates internal IDs.
    6. Re-converts to list-based structure and pushes the update.
    """
    client = get_client()

    if not os.path.exists(multipatch_file):
        console.print(f"[red]Error: Patch file not found: {multipatch_file}[/red]")
        raise typer.Exit(code=1)

    with open(multipatch_file, "r") as f:
        multipatch_data = json.load(f)

    forms_list = multipatch_data["forms"]

    # CONTINUE HERE

    target_form_id = None
    target_form_label = None
    if isinstance(multipatch_data, dict) and "patch" in multipatch_data:
        patch_list = multipatch_data["patch"]
        target_form_id = multipatch_data.get("form_id")
        target_form_label = multipatch_data.get("form_label")
    else:
        patch_list = multipatch_data

    # Reconstruct jsonpatch object
    patch = jsonpatch.JsonPatch(patch_list)

    if not target_form_id:
        # Determine targeted form from patch content or filename
        for op in patch_list:
            path = op.get("path", "")
            if path == "/id" and isinstance(op.get("value"), str):
                target_form_id = op.get("value")
                break

        if not target_form_id:
            patch_name = os.path.basename(multipatch_file)
            match = re.fullmatch(r"form_patch_([A-Za-z0-9]+)(?:\.json)?", patch_name)
            if match:
                target_form_id = match.group(1)

    if not target_form_id:
        console.print("[red]Error: Could not determine target form ID from patch or filename.[/red]")
        raise typer.Exit(code=1)

    # If we don't have the label yet, fetch the source schema to get it
    if not target_form_label:
        with handle_api_errors(f"Could not fetch source schema for form {target_form_id} to identify label"):
            source_schema = client.api.get_form_schema(target_form_id)
            target_form_label = source_schema.label

    planned_updates = []
    translator = SchemaIdTranslator(client, target_form_id)

    def build_compact_schema_preview(patched_schema_dict: dict, max_fields: int = 10) -> str:
        """Return a compact, human-readable snapshot of the patched schema."""
        elements = patched_schema_dict.get("elements", {}) or {}
        if not elements:
            return "(no fields)"

        lines = []
        for idx, (_, element) in enumerate(elements.items()):
            if idx >= max_fields:
                lines.append(f"... +{len(elements) - max_fields} more field(s)")
                break

            code = element.get("code") or "-"
            label = element.get("label") or "-"
            field_type = element.get("type") or "unknown"
            tags = []
            if element.get("key"):
                tags.append("key")
            if element.get("required"):
                tags.append("required")
            tag_text = f" [{' '.join(tags)}]" if tags else ""
            lines.append(f"{code} ({field_type}){tag_text} - {label}")

        return "\n".join(lines)

    def build_compact_change_preview(original_elements: dict, patched_elements: dict, max_lines: int = 8) -> str:
        """Return a compact change summary between original and patched element dicts."""
        added = [key for key in patched_elements if key not in original_elements]
        removed = [key for key in original_elements if key not in patched_elements]
        modified = [
            key for key in patched_elements
            if key in original_elements and patched_elements[key] != original_elements[key]
        ]

        if not added and not removed and not modified:
            return "(no field-level changes)"

        lines = []

        def render_line(prefix: str, key: str, source: dict) -> str:
            element = source.get(key, {})
            code = element.get("code") or key
            label = element.get("label") or "-"
            field_type = element.get("type") or "unknown"
            return f"{prefix} {code} ({field_type}) - {label}"

        for key in added:
            lines.append(render_line("+", key, patched_elements))
        for key in removed:
            lines.append(render_line("-", key, original_elements))
        for key in modified:
            lines.append(render_line("~", key, patched_elements))

        if len(lines) > max_lines:
            visible = lines[:max_lines]
            visible.append(f"... +{len(lines) - max_lines} more change(s)")
            return "\n".join(visible)

        return "\n".join(lines)

    with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
    ) as progress:
        task = progress.add_task("Preparing patch preview...", total=len(target_database_ids))

        for db_id in target_database_ids:
            progress.update(task, description=f"Simulating patch for {db_id}...")
            with handle_api_errors(f"Could not simulate patch for {db_id}"):
                # 0. Find form by label in target database
                target_tree = client.api.get_database_tree(db_id)
                target_form_res = next(
                    (res for res in target_tree.resources if
                     res.type == DatabaseTreeResourceType.FORM and res.label == target_form_label),
                    None
                )

                if not target_form_res:
                    console.print(f"[yellow]Skipping {db_id}: Form '{target_form_label}' not found.[/yellow]")
                    progress.advance(task)
                    continue

                actual_target_form_id = target_form_res.id

                # 1. Fetch current schema
                schema = client.api.get_form_schema(actual_target_form_id)
                schema_dict = schema.model_dump()

                # 2. Semantic conversion (list to dict keyed by code/id)
                schema_dict["elements"] = {e["code"] or e["id"]: e for e in schema_dict["elements"]}
                original_elements = schema_dict["elements"]

                # 3. Apply patch
                patched_dict = patch.apply(schema_dict)

                # Ensure the ID and databaseId are preserved for the target database
                patched_dict["id"] = actual_target_form_id
                patched_dict["databaseId"] = db_id

                report = translator.translate_schema(patched_dict, db_id)
                patched_dict = report.translated_schema
                patched_elements = patched_dict.get("elements", {})
                unresolved_count = len(report.unresolved_source_form_ids) + len(report.unresolved_source_field_ids)
                unresolved_preview = (
                    ", ".join(report.unresolved_source_form_ids[:3] + report.unresolved_source_field_ids[:3])
                    if unresolved_count else "-"
                )
                if unresolved_count > 6:
                    unresolved_preview = f"{unresolved_preview}, ..."
                planned_updates.append({
                    "db_id": db_id,
                    "form_id": actual_target_form_id,
                    "form_label": schema.label,
                    "change_preview": build_compact_change_preview(original_elements, patched_elements),
                    "schema_preview": build_compact_schema_preview(patched_dict),
                    "form_id_translations": report.form_id_replacements,
                    "field_id_translations": report.field_id_replacements,
                    "unresolved_count": unresolved_count,
                    "unresolved_preview": unresolved_preview,
                    "patched_schema_dict": patched_dict
                })
            progress.advance(task)

    if not planned_updates:
        console.print("[yellow]No target forms found matching the patch label.[/yellow]")
        return

    # --- Preview Table ---
    table = Table(title=f"Patch Preview: Form '{target_form_label}'")
    table.add_column("Database ID", style="cyan")
    table.add_column("Form ID", style="magenta")
    table.add_column("Form ID Translations", style="blue")
    table.add_column("Field ID Translations", style="blue")
    table.add_column("Unresolved IDs", style="red")
    table.add_column("Changes", style="yellow")
    table.add_column("Resulting Schema (Compact)", style="green")

    for up in planned_updates:
        table.add_row(
            up["db_id"],
            up["form_id"],
            str(up["form_id_translations"]),
            str(up["field_id_translations"]),
            f"{up['unresolved_count']} ({up['unresolved_preview']})" if up["unresolved_count"] else "0",
            up["change_preview"],
            up["schema_preview"]
        )

    console.print(table)

    if dry_run:
        console.print("\n[bold cyan]Dry run mode: No changes applied.[/bold cyan]")
        return

    blocked_updates = [up for up in planned_updates if up["unresolved_count"] > 0]
    if blocked_updates:
        console.print("\n[bold red]Cannot apply patch: unresolved source IDs remain after translation.[/bold red]")
        for up in blocked_updates:
            console.print(
                f"[red]- {up['db_id']}[/red]: {up['unresolved_count']} unresolved token(s) "
                f"({up['unresolved_preview']})"
            )
        console.print("[dim]Hint: Ensure equivalent forms/fields exist in target DB (matching labels/codes).[/dim]")
        raise typer.Exit(code=1)

    if not yes and not typer.confirm("\nApply these patches to all listed databases?"):
        raise typer.Abort()

    # --- Execution ---
    with console.status("Applying patches...") as status:
        for up in planned_updates:
            status.update(f"Updating {up['db_id']}...")
            # Reverse semantic conversion
            final_dict = up["patched_schema_dict"]
            final_dict["elements"] = list(final_dict["elements"].values())

            patched_schema = FormSchema.model_validate(final_dict)
            client.api.update_form_schema(patched_schema)
            console.print(
                f"[green]Successfully patched form '{up['form_label']}' ({up['form_id']}) in database {up['db_id']}.[/green]")

    console.print("\n[bold green]Batch application completed.[/bold green]")



if __name__ == "__main__":
    app()
