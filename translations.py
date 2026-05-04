import asyncio
import os
from typing import Annotated, Optional, List, Dict, Any, cast, Set, Tuple

import pandas as pd
import typer
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from activityinfo.client import UpdateDatabaseRequest, UpdateTranslationsRequest, TranslationString, \
    RecordUpdateRequest, RecordUpdateChange
from activityinfo.client.models.database_translation import DatabaseTranslation
from common import find_resource_by_prefix
from utils import get_client, console, handle_api_errors

# Initialize a Typer sub-application for translation management
app = typer.Typer(no_args_is_help=True)


@app.command(help="Sync translations from a source database to a target database.")
def transfer(
        source_database_id: Annotated[str, typer.Argument(help="The source ActivityInfo database ID")],
        target_database_id: Annotated[str, typer.Argument(help="The target ActivityInfo database ID")],
        language_code: Annotated[str, typer.Argument(help="The language code to sync (e.g., 'ar')")],
        dry_run: Annotated[bool, typer.Option(help="Preview changes without applying them")] = False
):
    """
    Copy database and form-level translations from one database to another.
    Matches forms by label.
    """
    asyncio.run(_transfer_async(source_database_id, target_database_id, language_code, dry_run))


async def _transfer_async(
        source_database_id: str,
        target_database_id: str,
        language_code: str,
        dry_run: bool
):
    client = get_client()

    with Progress(SpinnerColumn(),
                  TextColumn("[progress.description]{task.description}"),
                  BarColumn(),
                  TaskProgressColumn(),
                  console=console) as progress:

        # --- 1. Initialization & Data Retrieval ---
        task = progress.add_task("Fetching database structures...", total=None)

        with handle_api_errors("Could not retrieve database structures"):
            source_tree = await client.get_database_tree(database_id=source_database_id)
            target_tree = await client.get_database_tree(database_id=target_database_id)
            source_db_translations = await client.get_database_translations(database_id=source_database_id,
                                                                            language_code=language_code)

        # --- 2. Validation ---
        if language_code not in source_tree.languages:
            console.print(f"[red]Error:[/red] Language '{language_code}' not found in source database.")
            raise typer.Exit(code=1)

        # --- 3. Process Database Level ---
        progress.update(task, description=f"Processing database-level translations...")

        # --- 4. Language Setup ---
        target_languages = target_tree.languages or []
        if language_code not in target_languages:
            progress.update(task, description=f"Adding '{language_code}' to target database...")
            if not dry_run:
                with handle_api_errors(f"Could not add language '{language_code}' to target database"):
                    await client.update_database(database_id=target_database_id,
                                                 update_database_request=UpdateDatabaseRequest(
                                                     resourceUpdates=[],
                                                     resourceDeletions=[],
                                                     languageUpdates=[language_code],
                                                     originalLanguage="en"
                                                 ))

        # --- 5. Sync Database Strings ---
        if source_db_translations and hasattr(source_db_translations, "translated_strings"):
            mapped_db_strings = [
                DatabaseTranslation(
                    id=t.id,
                    original=t.original,
                    translated=t.translated,
                    autoTranslated=t.auto_translated
                ) for t in source_db_translations.translated_strings
            ]
            if not dry_run:
                with handle_api_errors("Failed to sync database-level translations"):
                    await client.update_database_translations(
                        database_id=target_database_id,
                        language_code=language_code,
                        update_translations_request=
                        UpdateTranslationsRequest(strings=mapped_db_strings)
                    )

        # --- 6. Sync Form-Level Strings ---
        target_forms = [res for res in target_tree.resources if res.type == "FORM"]
        progress.update(task, description="Syncing form translations...", total=len(target_forms))

        for form in target_forms:
            source_form = next((res for res in source_tree.resources if
                                res.label == form.label and res.type == "FORM"), None)
            if not source_form:
                progress.advance(task)
                continue

            with handle_api_errors(f"Failed to sync translations for {form.label}"):
                source_translations = await client.get_form_translations(database_id=source_database_id,
                                                                         form_id=source_form.id,
                                                                         language_code=language_code)
                # source_schema = await client.get_form_schema_get(source_form.id)
                # target_schema = await client.get_form_schema_get(form.id)

                if not dry_run and source_translations and hasattr(source_translations, "translated_strings"):
                    mapped_form_strings = [
                        TranslationString(
                            id=t.id,
                            original=t.original,
                            translated=t.translated,
                            autoTranslated=t.auto_translated
                        ) for t in source_translations.translated_strings
                    ]
                    await client.update_form_schema_translations(
                        form_id=form.id,
                        language_code=language_code,
                        update_translations_request=UpdateTranslationsRequest(strings=mapped_form_strings)
                    )

            progress.advance(task)

    console.print(f"[bold green]Sync to {language_code} completed successfully.[/bold green]")


@app.command(help="Upsert translations from a CSV file into a database.")
def upsert(
        target_database_id: Annotated[str, typer.Argument(help="The target ActivityInfo database ID")],
        input_file: Annotated[str, typer.Argument(help="Path to the CSV file with translations")],
        language_code: Annotated[str, typer.Argument(help="The language code (e.g., 'ar')")],
        dry_run: Annotated[bool, typer.Option(help="Preview changes without applying them")] = False
):
    """
    Reads a CSV (Original, Translated) and updates form schemas and database translations.
    Automatically handles adding the language if missing.
    """
    asyncio.run(_upsert_async(target_database_id, input_file, language_code, dry_run))


async def _upsert_async(
        target_database_id: str,
        input_file: str,
        language_code: str,
        dry_run: bool
):
    client = get_client()

    if not os.path.exists(input_file):
        console.print(f"[red]Error:[/red] File not found: {input_file}")
        raise typer.Exit(code=1)

    df = pd.read_csv(input_file)
    # Expected columns: Original, Translated
    translation_map = dict(zip(df['Original'], df['Translated']))

    def get_translation(text: str) -> Optional[str]:
        if pd.isna(text): return None
        return translation_map.get(text.strip())

    with Progress(SpinnerColumn(),
                  TextColumn("[progress.description]{task.description}"),
                  BarColumn(),
                  TaskProgressColumn(),
                  console=console) as progress:
        task = progress.add_task("Fetching database structure...", total=None)

        with handle_api_errors("Could not retrieve database structure"):
            target_tree = await client.get_database_tree(database_id=target_database_id)

        if target_tree.original_language and target_tree.original_language.lower() != "en":
            console.print(
                f"[bold red]Error:[/bold red] Target DB original language must be 'en', found '{target_tree.original_language}'")
            raise typer.Exit(code=1)

        # --- Language Setup ---
        target_languages = target_tree.languages or []
        if language_code not in target_languages:
            progress.update(task, description=f"Adding '{language_code}' to target database...")
            if not dry_run:
                with handle_api_errors(f"Could not add language '{language_code}' to target database"):
                    await client.update_database(database_id=target_database_id,
                                                 update_database_request=UpdateDatabaseRequest(
                                                     resourceUpdates=[],
                                                     resourceDeletions=[],
                                                     languageUpdates=[language_code],
                                                     originalLanguage="en"
                                                 ))

        # --- Prep Tracking ---
        schema_missing_en: Set[Tuple[str, str, str, str]] = set()
        db_strings_to_update: List[DatabaseTranslation] = []

        # --- 1. Database level (Roles) ---
        for role in target_tree.roles:
            trans = get_translation(role.label)
            if trans:
                db_strings_to_update.append(DatabaseTranslation(id=f"role:{role.id}:label", original=role.label,
                                                                translated=trans, autoTranslated=False))
            else:
                schema_missing_en.add(("Database", "Role", "role:label", role.label))

        # --- 2. Resources (Folders & Forms) ---
        target_forms = [res for res in target_tree.resources if res.type == "FORM"]
        target_folders = [res for res in target_tree.resources if res.type == "FOLDER"]

        if not dry_run:
            for folder in target_folders:
                trans = get_translation(folder.label)
                if trans:
                    await client.update_database_translations(database_id=target_database_id,
                                                              language_code=language_code,
                                                              update_translations_request=UpdateTranslationsRequest(
                                                                  strings=[
                                                                      TranslationString(
                                                                          id=f"resource:{folder.id}:label",
                                                                          original=folder.label,
                                                                          translated=trans,
                                                                          autoTranslated=False)
                                                                  ]))
                else:
                    schema_missing_en.add(("Database", "Folder", "resource:label", folder.label))

            for form in progress.track(target_forms, description="Syncing form schema translations"):
                with handle_api_errors(f"Failed to sync translations for {form.label}"):
                    schema = await client.get_form_schema(form_id=form.id)

                    form_trans_val = get_translation(form.label)
                    form_strings = [TranslationString(id=f"resource:{form.id}:label", original=form.label,
                                                      translated=form_trans_val or "", autoTranslated=False)]
                    if not form_trans_val:
                        schema_missing_en.add(("Form", form.label, "resource:label", form.label))

                    for field in schema.elements:
                        # Field Label
                        f_trans = get_translation(field.label)
                        if f_trans:
                            form_strings.append(TranslationString(id=f"resource:{form.id}:field:{field.id}:label",
                                                                  original=field.label, translated=f_trans,
                                                                  autoTranslated=False))
                        else:
                            schema_missing_en.add(("Field", form.label, field.code or field.id, field.label))

                        # Field Description (if exists)
                        if hasattr(field, "description") and field.description:
                            d_trans = get_translation(field.description)
                            if d_trans:
                                form_strings.append(
                                    TranslationString(id=f"resource:{form.id}:field:{field.id}:description",
                                                      original=field.description, translated=d_trans,
                                                      autoTranslated=False))

                        # Choice values
                        if field.type_parameters and hasattr(field.type_parameters,
                                                             "values") and field.type_parameters.values:
                            for val in field.type_parameters.values:
                                v_trans = get_translation(val.label)
                                if v_trans:
                                    form_strings.append(
                                        TranslationString(id=f"resource:{form.id}:field:{field.id}:value:{val.id}",
                                                          original=val.label, translated=v_trans,
                                                          autoTranslated=False))
                                else:
                                    schema_missing_en.add(("Choice", form.label, field.code or field.id, val.label))

                    await client.update_form_schema_translations(form_id=form.id, language_code=language_code,
                                                                 update_translations_request=UpdateTranslationsRequest(
                                                                     strings=form_strings))

    # --- 3. Reporting ---
    if schema_missing_en:
        console.print(f"\n[yellow]Warning:[/yellow] {len(schema_missing_en)} strings missing from translation file:")
        # for item in sorted(list(schema_missing_en)):
        #    console.print(f"  - {item}")

    console.print(f"\n[bold green]Upsert of {language_code} completed.[/bold green]")


@app.command(help="Apply record-level translations using a reference form map.")
def records(
        target_database_id: Annotated[str, typer.Argument(help="The target database ID")],
        config_form_prefix: Annotated[str, typer.Option(help="Prefix for translation config form")] = "0.1.4",
        language_code: Annotated[str, typer.Option(help="Language code target")] = "ar",
        dry_run: Annotated[bool, typer.Option(help="Dry run")] = False
):
    """
    Translates data records based on a mapping form (0.1.4).
    Matches records in target forms by looking up their current English value 
    in a reference 'Translation Map' form.
    """
    asyncio.run(_records_async(target_database_id, config_form_prefix, language_code, dry_run))


async def _records_async(
        target_database_id: str,
        config_form_prefix: str,
        language_code: str,
        dry_run: bool
):
    client = get_client()

    with handle_api_errors("Could not get tree"):
        tree = await client.get_database_tree(database_id=target_database_id)

    config_form = find_resource_by_prefix(tree.resources, config_form_prefix)
    if not config_form:
        console.print(f"[red]Error:[/red] Config form {config_form_prefix} not found.")
        raise typer.Exit(1)

    # 1. Load the Map
    with handle_api_errors("Could not load translation map"):
        map_records = cast(List[Dict[str, Any]], await client.get_form_records(form_id=config_form.id))
        # Map: EN_VALUE -> AR_VALUE
        translation_map = {str(r.get("EN")).strip(): r.get("AR") for r in map_records if r.get("EN")}

    # 2. Identify forms that need translation (prefixed 3, 4, 5, 6)
    data_forms = [res for res in tree.resources if res.type == "FORM" and res.label[0] in "3456"]

    not_found_en = set()

    for form_res in data_forms:
        with handle_api_errors(f"Processing form {form_res.label}"):
            schema = await client.get_form_schema(form_id=form_res.id)
            records = cast(List[Dict[str, Any]], await client.get_form_records(form_id=form_res.id))

            # Find translatable fields (Free text)
            trans_fields = [f for f in schema.elements if f.type == "free_text"]
            if not trans_fields: continue

            updates: List[RecordUpdateChange] = []

            for rec in records:
                changes = {}
                for field in trans_fields:
                    val = rec.get(field.code or field.id)
                    if val and isinstance(val, str):
                        translated = translation_map.get(val.strip())
                        if translated:
                            changes[field.code or field.id] = translated
                        else:
                            if val.strip(): not_found_en.add(val.strip())

                if changes:
                    updates.append(RecordUpdateChange(
                        formId=form_res.id,
                        recordId=cast(str, rec["@id"]),
                        fields=changes
                    ))

            if updates and not dry_run:
                console.print(f"Applying {len(updates)} translations to {form_res.label}...")
                await client.update_form_records(record_update_request=RecordUpdateRequest(changes=updates))

    if not_found_en:
        console.print(f"\n[yellow]Warning:[/yellow] {len(not_found_en)} values not found in map.")
        pd.DataFrame({"Missing English": list(not_found_en)}).to_csv("missing_translations.csv", index=False)

    console.print("[bold green]Record translation complete.[/bold green]")


if __name__ == "__main__":
    app()
