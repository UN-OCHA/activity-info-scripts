import csv
from typing import Annotated, Optional

import pandas as pd
import typer
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from api.models import RecordUpdateDTO
from api.models import UpdateDatabaseDTO, DatabaseTreeResourceType, UpdateDatabaseTranslationsDTO, DatabaseTranslation
from common import get_records_with_multiref
from utils import get_client, console, handle_api_errors

# Initialize a Typer sub-application for translation management
app = typer.Typer(no_args_is_help=True)


@app.command(help="Migrate existing translations from a source database to a target database.")
def transfer(
        source_database_id: Annotated[str, typer.Argument(help="The ActivityInfo ID of the origin database")],
        target_database_id: Annotated[str, typer.Argument(help="The ActivityInfo ID of the target database")],
        language_code: Annotated[str, typer.Argument(help="The two-letter ISO language code (e.g., 'fr', 'es')")],
        dry_run: Annotated[bool, typer.Option(help="Do not actually perform the transfer")] = False,
):
    """
    Synchronize translations for a specific language between two ActivityInfo databases.
    
    This command fetches translations from a source database and maps them onto the 
    corresponding forms and fields of a target database based on matching labels.
    """
    client = get_client()

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
                  BarColumn(),
                  TaskProgressColumn(),
                  console=console) as progress:

        # --- 1. Initialization & Data Retrieval ---
        task = progress.add_task("Fetching database structures...", total=None)

        with handle_api_errors("Could not retrieve database structures"):
            source_tree = client.api.get_database_tree(source_database_id)
            target_tree = client.api.get_database_tree(target_database_id)
            source_db_translations = client.api.get_database_translations(source_database_id, language_code)

        # --- 2. Validation ---
        if target_tree.originalLanguage and target_tree.originalLanguage.lower() != "en":
            console.print(
                f"[bold red]Error:[/bold red] Target DB original language must be 'en', found '{target_tree.originalLanguage}'")
            raise typer.Exit(code=1)

        # --- 3. Build Resource ID Mapping ---
        # Map source IDs to target IDs based on label and type compatibility
        resource_id_map = {source_database_id: target_database_id}

        # Create lookups for target resources by (type, label)
        target_resources_by_key = {(res.type, res.label): res for res in target_tree.resources}

        for s_res in source_tree.resources:
            key = (s_res.type, s_res.label)
            if key in target_resources_by_key:
                resource_id_map[s_res.id] = target_resources_by_key[key].id

        def map_identifier(res_id: str, field_id_map: Optional[dict[str, str]] = None) -> str:
            """
            Robustly maps a translation identifier (resource or field) from source to target.
            Example: 'resource:SOURCE_DB:label' -> 'resource:TARGET_DB:label'
            Example: 'field:SOURCE_FIELD:label' -> 'field:TARGET_FIELD:label'
            """
            parts = res_id.split(":")
            if not parts:
                return res_id

            prefix = parts[0]
            if prefix == "resource" and len(parts) >= 2:
                source_id = parts[1]
                parts[1] = resource_id_map.get(source_id, source_id)
                return ":".join(parts)
            elif prefix == "field" and len(parts) >= 2 and field_id_map:
                source_id = parts[1]
                parts[1] = field_id_map.get(source_id, source_id)
                return ":".join(parts)
            return res_id

        # --- 4. Language Setup ---
        if language_code not in target_tree.languages:
            progress.update(task, description=f"Adding '{language_code}' to target database...")
            if not dry_run:
                with handle_api_errors(f"Could not add language '{language_code}' to target database"):
                    client.api.update_database(target_database_id, UpdateDatabaseDTO(
                        resourceUpdates=[],
                        resourceDeletions=[],
                        languageUpdates=[language_code],
                        originalLanguage="en"
                    ))

        # --- 5. Sync Database-Level Translations ---
        progress.update(task, description="Syncing database-level translations...")
        if not dry_run:
            mapped_db_strings = [
                DatabaseTranslation(
                    id=map_identifier(t.id),
                    original=t.original,
                    translated=t.translated,
                    autoTranslated=t.auto_translated
                ) for t in source_db_translations.translated_strings
            ]
            with handle_api_errors("Failed to sync database-level translations"):
                client.api.update_database_translations(
                    target_database_id,
                    language_code,
                    UpdateDatabaseTranslationsDTO(strings=mapped_db_strings)
                )

        # --- 6. Sync Form-Level Translations ---
        target_forms = [res for res in target_tree.resources if res.type == DatabaseTreeResourceType.FORM]
        progress.update(task, description="Syncing form-level translations...", total=len(target_forms))

        for form in target_forms:
            progress.update(task, description=f"Processing form: {form.label}")

            # Find matching source form
            source_form = next((res for res in source_tree.resources if
                                res.label == form.label and res.type == DatabaseTreeResourceType.FORM), None)

            if not source_form:
                progress.advance(task)
                continue

            with handle_api_errors(f"Failed to sync translations for {form.label}"):
                # Fetch translations and schemas
                source_translations = client.api.get_form_translations(source_database_id, source_form.id,
                                                                       language_code)
                source_schema = client.api.get_form_schema(source_form.id)
                target_schema = client.api.get_form_schema(form.id)

                # Map field IDs by label
                target_fields_by_label = {f.label: f for f in target_schema.elements}
                field_id_map = {}
                for s_field in source_schema.elements:
                    if s_field.label in target_fields_by_label:
                        field_id_map[s_field.id] = target_fields_by_label[s_field.label].id

                if not dry_run:
                    new_translations = [
                        DatabaseTranslation(
                            id=map_identifier(t.id, field_id_map if 'field_id_map' in locals() else None),
                            original=t.original,
                            translated=t.translated,
                            autoTranslated=t.auto_translated
                        )
                        for t in source_translations.translated_strings
                    ]

                    client.api.update_form_translations(
                        target_database_id, form.id, language_code,
                        UpdateDatabaseTranslationsDTO(strings=new_translations)
                    )

            progress.advance(task)

    # Final summary output
    if dry_run:
        console.print("[bold cyan]Dry run completed successfully. No changes were made.[/bold cyan]")
    else:
        console.print("[bold green]Transfer executed successfully.[/bold green]")


@app.command(help="Upsert existing translations from an Excel file to a target database.")
def upsert(
        source_translations_file: Annotated[
            str, typer.Argument(help="The local path of the Excel file containing translations to upsert")],
        target_database_id: Annotated[str, typer.Argument(help="The ActivityInfo ID of the target database")],
        language_code: Annotated[str, typer.Argument(help="The two-letter ISO language code (e.g., 'fr', 'es')")],
        replace_translations: Annotated[bool, typer.Option(help="Overwrite existing translations")] = False,
        skip_schema: Annotated[bool, typer.Option(help="Skip schema translations")] = False,
        schema_output: Annotated[Optional[str], typer.Option(
            help="Output file containing missing ActivityInfo schema translations")] = "schema_output.csv",
        skip_reference: Annotated[bool, typer.Option(help="Skip reference translations")] = False,
        reference_output: Annotated[Optional[str], typer.Option(
            help="Output file containing missing ActivityInfo reference translations")] = "reference_output.csv",
):
    """
    Upsert translations for a specific language between an Excel file and a target ActivityInfo database.
    """

    client = get_client()
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
                  BarColumn(),
                  TaskProgressColumn(),
                  console=console) as progress:
        task = progress.add_task("Fetching database structure...", total=None)

        with handle_api_errors("Could not retrieve database structure"):
            target_tree = client.api.get_database_tree(target_database_id)

        if target_tree.originalLanguage and target_tree.originalLanguage.lower() != "en":
            console.print(
                f"[bold red]Error:[/bold red] Target DB original language must be 'en', found '{target_tree.originalLanguage}'")
            raise typer.Exit(code=1)

        # --- Language Setup ---
        if language_code not in target_tree.languages:
            progress.update(task, description=f"Adding '{language_code}' to target database...")
            with handle_api_errors(f"Could not add language '{language_code}' to target database"):
                client.api.update_database(target_database_id, UpdateDatabaseDTO(
                    resourceUpdates=[],
                    resourceDeletions=[],
                    languageUpdates=[language_code],
                    originalLanguage="en"
                ))

        schema_missing_en = set()

        # --- SCHEMA TRANSLATIONS ---
        if not skip_schema:
            progress.update(task, description="Processing schema translations...")
            try:
                # Header is on the second row (index 1)
                df_schema = pd.read_excel(source_translations_file, sheet_name='CM - Schema', header=1)
            except ValueError:
                console.print("[bold red]Error:[/bold red] 'CM - Schema' sheet missing from Excel file.")
                raise typer.Exit(code=1)

            if 'EN' not in df_schema.columns:
                console.print("[bold red]Error:[/bold red] 'EN' column missing from 'CM - Schema' sheet.")
                raise typer.Exit(code=1)

            target_lang_col = language_code.upper()
            if target_lang_col not in df_schema.columns:
                console.print(
                    f"[bold red]Error:[/bold red] '{target_lang_col}' column missing from 'CM - Schema' sheet.")
                raise typer.Exit(code=1)

            # Create mapping EN -> Target, ensuring strings and dropping empty EN or Target values
            schema_map = df_schema.dropna(subset=['EN', target_lang_col]).set_index('EN')[target_lang_col].to_dict()
            # Convert keys and values to strings
            schema_map = {str(k).strip(): str(v).strip() for k, v in schema_map.items()}

            # Database-level translations
            progress.update(task, description="Syncing database-level translations...")
            with handle_api_errors("Failed to get database-level translations"):
                db_translations = client.api.get_database_translations(target_database_id, language_code)

            updated_db_strings = []
            for t in db_translations.translated_strings:
                original_clean = t.original.strip()
                if original_clean in schema_map:
                    target_val = schema_map[original_clean]
                    if replace_translations or not t.translated:
                        t.translated = target_val
                        updated_db_strings.append(t)
                else:
                    schema_missing_en.add(t.original)

            if updated_db_strings:
                with handle_api_errors("Failed to update database-level translations"):
                    client.api.update_database_translations(
                        target_database_id,
                        language_code,
                        UpdateDatabaseTranslationsDTO(strings=updated_db_strings)
                    )

            # Form-level translations
            target_forms = [res for res in target_tree.resources if res.type == DatabaseTreeResourceType.FORM]
            for form in progress.track(target_forms, description="Syncing form schema translations"):
                with handle_api_errors(f"Failed to sync translations for {form.label}"):
                    form_translations = client.api.get_form_translations(target_database_id, form.id, language_code)
                    updated_form_strings = []
                    for t in form_translations.translated_strings:
                        original_clean = t.original.strip()
                        if original_clean in schema_map:
                            target_val = schema_map[original_clean]
                            if replace_translations or not t.translated:
                                t.translated = target_val
                                updated_form_strings.append(t)
                        else:
                            schema_missing_en.add(t.original)

                    if updated_form_strings:
                        client.api.update_form_translations(
                            target_database_id, form.id, language_code,
                            UpdateDatabaseTranslationsDTO(strings=updated_form_strings)
                        )

            console.print(f"[bold green]Schema translations completed.[/bold green]")

            if schema_output and schema_missing_en:
                with open(schema_output, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(['EN'])
                    for val in sorted(schema_missing_en):
                        writer.writerow([val])
                console.print(f"Missing schema EN values written to {schema_output}")

        # --- REFERENCE VALUE TRANSLATIONS ---
        if not skip_reference:
            progress.update(task, description="Processing reference value translations...")
            try:
                # Header is on the second row (index 1)
                df_values = pd.read_excel(source_translations_file, sheet_name='CM - Values', header=1)
            except ValueError:
                console.print("[bold red]Error:[/bold red] 'CM - Values' sheet missing from Excel file.")
                raise typer.Exit(code=1)

            required_cols = ['Form System Prefix', 'Field Code', 'Refcode']
            for col in required_cols:
                if col not in df_values.columns:
                    console.print(f"[bold red]Error:[/bold red] '{col}' column missing from 'CM - Values' sheet.")
                    raise typer.Exit(code=1)

            target_lang_col = language_code.upper()
            if target_lang_col not in df_values.columns:
                console.print(
                    f"[bold red]Error:[/bold red] '{target_lang_col}' column missing from 'CM - Values' sheet.")
                raise typer.Exit(code=1)

            # Target field name (e.g., NAME_SO)
            target_field_suffix = f"_{language_code.upper()}"

            not_found_items = []

            # Group by Form System Prefix to minimize form fetching, treating prefixes as strings
            df_values['Form System Prefix'] = df_values['Form System Prefix'].astype(str)
            grouped = df_values.groupby('Form System Prefix')

            for prefix, group in progress.track(grouped, description="Syncing reference values"):
                # Find form by prefix
                form_res = next((res for res in target_tree.resources if
                                 res.type == DatabaseTreeResourceType.FORM and res.label.startswith(str(prefix))), None)
                if not form_res:
                    for _, row in group.iterrows():
                        not_found_items.append({
                            'Form System Prefix': prefix,
                            'Field Code': row['Field Code'],
                            'Refcode': row['Refcode'],
                            'Reason': 'Form not found'
                        })
                    continue

                with handle_api_errors(f"Failed to fetch data for form {form_res.label}"):
                    schema = client.api.get_form_schema(form_res.id)
                    records = get_records_with_multiref(client, form_res.id)

                    # Create lookup for records by Refcode (checking CCODE then REFCODE)
                    record_map = {}
                    for rec in records:
                        ccode = str(rec.get('CCODE', '')).strip()
                        refcode = str(rec.get('REFCODE', '')).strip()
                        if ccode:
                            record_map[ccode] = rec
                        if refcode and refcode not in record_map:
                            record_map[refcode] = rec

                    updates = []
                    for _, row in group.iterrows():
                        field_code = str(row['Field Code']).strip()
                        refcode = str(row['Refcode']).strip()

                        target_field_code = f"{field_code}{target_field_suffix}"

                        # Check if field exists in schema
                        field_exists = any(f.code == target_field_code for f in schema.elements)
                        if not field_exists:
                            not_found_items.append({
                                'Form System Prefix': prefix,
                                'Field Code': field_code,
                                'Refcode': refcode,
                                'Reason': f'Field {target_field_code} not found'
                            })
                            continue

                        # Find record
                        rec = record_map.get(refcode)
                        if not rec:
                            not_found_items.append({
                                'Form System Prefix': prefix,
                                'Field Code': field_code,
                                'Refcode': refcode,
                                'Reason': 'Record not found'
                            })
                            continue

                        # Translation value
                        trans_val = row.get(target_lang_col)
                        if pd.isna(trans_val) or str(trans_val).strip() == "":
                            continue

                        # Check ReplaceTranslations logic
                        existing_val = rec.get(target_field_code)
                        if replace_translations or not existing_val:
                            updates.append(RecordUpdateDTO(
                                formId=form_res.id,
                                recordId=rec['@id'],
                                fields={target_field_code: str(trans_val).strip()}
                            ))

                    if updates:
                        # Chunk updates if they are too many (e.g., > 100)
                        for i in range(0, len(updates), 100):
                            client.api.update_form_records(updates[i:i + 100])

            console.print(f"[bold green]Reference value translations completed.[/bold green]")

            if reference_output and not_found_items:
                with open(reference_output, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=['Form System Prefix', 'Field Code', 'Refcode', 'Reason'])
                    writer.writeheader()
                    writer.writerows(not_found_items)
                console.print(f"Not found items written to {reference_output}")

    console.print("[bold green]Upsert executed successfully.[/bold green]")


# Standard Python entry point
if __name__ == "__main__":
    app()
