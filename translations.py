from typing import Annotated

import typer
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from api.models import UpdateDatabaseDTO, DatabaseTreeResourceType, UpdateDatabaseTranslationsDTO, DatabaseTranslation
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

    # Use a rich Progress bar to provide visual feedback for the multi-step migration
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
                  BarColumn(),
                  TaskProgressColumn(),
                  console=console) as progress:

        # --- 1. Initialization ---
        task = progress.add_task("Fetching source translations...", total=None)

        # --- 2. Retrieve Source Data ---
        # Fetch all translated strings for the requested language from the source database
        with handle_api_errors(f"Could not retrieve '{language_code}' translations for DB {source_database_id}"):
            source_db_translations = client.api.get_database_translations(source_database_id, language_code)

        # --- 3. Retrieve Target Structure ---
        # Get the full tree of the target database to understand its forms and hierarchy
        progress.update(task, description="Fetching target database structure...")
        with handle_api_errors(f"Could not retrieve target database tree for {target_database_id}"):
            target_tree = client.api.get_database_tree(target_database_id)

        # --- 4. Validation ---
        # Ensure the target database is set up with English as the base language
        if target_tree.originalLanguage and target_tree.originalLanguage.lower() != "en":
            console.print(
                f"[bold red]Error:[/bold red] Target DB original language must be 'en', found '{target_tree.originalLanguage}'")
            raise typer.Exit(code=1)

        # --- 5. Language Setup ---
        # If the target language is not already enabled in the target database, add it
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

        # --- 6. Sync Database-Level Translations ---
        # Apply global database translations (e.g., database label, folder names)
        progress.update(task, description="Syncing database-level translations...")
        if not dry_run:
            with handle_api_errors("Failed to sync database-level translations"):
                client.api.update_database_translations(target_database_id, language_code, source_db_translations)

        # --- 7. Sync Form-Level Translations ---
        # Identify all forms in the target database
        target_forms = [res for res in target_tree.resources if res.type == DatabaseTreeResourceType.FORM]
        progress.update(task, description="Syncing form-level translations...", total=len(target_forms))

        for form in target_forms:
            form_identifier = form.label
            progress.update(task, description=f"Processing form: {form_identifier}")

            # 7.1 Match Source Form
            # Retrieve the source database tree to find a form with the same label
            source_tree = client.api.get_database_tree(source_database_id)
            source_form = next((res for res in source_tree.resources if
                                res.label == form.label and res.type == DatabaseTreeResourceType.FORM), None)

            if not source_form:
                # If no matching form is found by label, skip to the next one
                progress.advance(task)
                continue

            # 7.2 Fetch Schemas for Field Mapping
            with handle_api_errors(f"Failed to sync translations for {form_identifier}"):
                existing_translations = client.api.get_form_translations(source_database_id, source_form.id,
                                                                         language_code)
                source_schema = client.api.get_form_schema(source_form.id)
                target_schema = client.api.get_form_schema(form.id)

                # 7.3 Map Elements by Label
                # Create lookups to resolve field IDs between the two different databases
                source_fields_by_label = {f.label: f for f in source_schema.elements}
                target_fields_by_label = {f.label: f for f in target_schema.elements}

                # 7.4. Identifier Mapping Logic
                def update_identifier(res_id: str) -> str:
                    """
                    Maps a translation identifier (form or field) from source ID to target ID.
                    Example: 'field:abc1234:label' -> 'field:xyz5678:label'
                    """
                    if res_id.startswith("resource:"):
                        # Replace source form ID with target form ID
                        return res_id.replace(source_form.id, form.id)
                    elif res_id.startswith("field:"):
                        # Extract the source field ID from the composite identifier
                        parts = res_id.split(":")
                        source_field_id = parts[1]

                        # Find the source field by its ID to get its human-readable label
                        source_field = next((f for f in source_schema.elements if f.id == source_field_id), None)
                        if not source_field:
                            return res_id

                        # Find the corresponding field in the target database using that same label
                        target_field = target_fields_by_label.get(source_field.label)
                        if not target_field:
                            return res_id

                        # Swap the ID in the identifier string
                        return res_id.replace(source_field_id, target_field.id)
                    return res_id

                # 7.5. Apply Mapped Translations
                if not dry_run:
                    # Construct the list of new translations with remapped IDs
                    new_translations = [DatabaseTranslation(
                        id=update_identifier(t.id),
                        original=t.original,
                        translated=t.translated,
                        autoTranslated=t.auto_translated
                    ) for t in existing_translations.translated_strings]

                    # Push the mapped translations to the target form
                    client.api.update_form_translations(
                        form.id, language_code,
                        UpdateDatabaseTranslationsDTO(strings=new_translations)
                    )

            # Update progress bar
            progress.advance(task)

    # Final summary output
    if dry_run:
        console.print("[bold cyan]Dry run completed successfully. No changes were made.[/bold cyan]")
    else:
        console.print("[bold green]Transfer executed successfully.[/bold green]")


# Standard Python entry point
if __name__ == "__main__":
    app()
