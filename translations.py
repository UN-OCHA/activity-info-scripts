from typing import Annotated, Optional

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
                source_translations = client.api.get_form_translations(source_database_id, source_form.id, language_code)
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
                        form.id, language_code,
                        UpdateDatabaseTranslationsDTO(strings=new_translations)
                    )

            progress.advance(task)

    # Final summary output
    if dry_run:
        console.print("[bold cyan]Dry run completed successfully. No changes were made.[/bold cyan]")
    else:
        console.print("[bold green]Transfer executed successfully.[/bold green]")


# Standard Python entry point
if __name__ == "__main__":
    app()
