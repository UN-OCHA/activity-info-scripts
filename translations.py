from typing import Annotated

import typer
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from api.models import UpdateDatabaseDTO, DatabaseTreeResourceType, UpdateDatabaseTranslationsDTO, DatabaseTranslation
from utils import get_client, console, handle_api_errors

app = typer.Typer(no_args_is_help=True)


@app.command(help="Migrate existing translations from a source database to a target database.")
def transfer(
        source_database_id: Annotated[str, typer.Argument(help="The ActivityInfo ID of the origin database")],
        target_database_id: Annotated[str, typer.Argument(help="The ActivityInfo ID of the target database")],
        language_code: Annotated[str, typer.Argument(help="The two-letter ISO language code")],
        dry_run: Annotated[bool, typer.Option(help="Do not actually perform the transfer")] = False,
):
    client = get_client()

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
                  BarColumn(),
                  TaskProgressColumn(),
                  console=console) as progress:
        # 1. Initialize Task
        task = progress.add_task("Fetching source translations...", total=None)

        # 2. Get Source translations
        with handle_api_errors(f"Could not retrieve '{language_code}' translations for DB {source_database_id}"):
            source_db_translations = client.api.get_database_translations(source_database_id, language_code)

        # 3. Get Target tree
        progress.update(task, description="Fetching target database structure...")
        with handle_api_errors(f"Could not retrieve target database tree for {target_database_id}"):
            target_tree = client.api.get_database_tree(target_database_id)

        # 4. Language validation and setup
        if target_tree.originalLanguage and target_tree.originalLanguage.lower() != "en":
            console.print(f"[bold red]Error:[/bold red] Target DB original language must be 'en', found '{target_tree.originalLanguage}'")
            raise typer.Exit(code=1)

        # 5. Ensure language exists in target
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

        # 6. Database Level Translations
        progress.update(task, description="Syncing database-level translations...")
        if not dry_run:
            with handle_api_errors("Failed to sync database-level translations"):
                client.api.update_database_translations(target_database_id, language_code, source_db_translations)

        # 7. Form Level Translations
        # Filter forms in target tree
        target_forms = [res for res in target_tree.resources if res.type == DatabaseTreeResourceType.FORM]
        progress.update(task, description="Syncing form-level translations...", total=len(target_forms))

        for form in target_forms:
            form_identifier = form.label
            progress.update(task, description=f"Processing form: {form_identifier}")

            # 7.1 Find matching form in source
            # Ideally we'd have the source tree too, for now we assume names match
            # But the spec says we should find the form in the source DB
            # Let's get the source tree to be accurate
            source_tree = client.api.get_database_tree(source_database_id)
            source_form = next((res for res in source_tree.resources if res.label == form.label and res.type == DatabaseTreeResourceType.FORM), None)

            if not source_form:
                progress.advance(task)
                continue

            # 7.2 Fetch schemas and translations
            with handle_api_errors(f"Failed to sync translations for {form_identifier}"):
                existing_translations = client.api.get_form_translations(source_database_id, source_form.id,
                                                                         language_code)
                source_schema = client.api.get_form_schema(source_form.id)
                target_schema = client.api.get_form_schema(form.id)

                # 7.3 Map elements by label
                # In AI, fields are matched by label/path. For simplicity here, we map by code or label.
                source_fields_by_label = {f.label: f for f in source_schema.elements}
                target_fields_by_label = {f.label: f for f in target_schema.elements}

                # 7.4. Update identifiers in translations
                def update_identifier(res_id: str) -> str:
                    if res_id.startswith("resource:"):
                        # Map resource:formId
                        return res_id.replace(source_form.id, form.id)
                    elif res_id.startswith("field:"):
                        # Map field:fieldId:label etc
                        parts = res_id.split(":")
                        source_field_id = parts[1]
                        
                        # Find the source field by ID to get its label
                        source_field = next((f for f in source_schema.elements if f.id == source_field_id), None)
                        if not source_field:
                            return res_id
                        
                        # Find the corresponding target field by the same label
                        target_field = target_fields_by_label.get(source_field.label)
                        if not target_field:
                            return res_id
                        
                        return res_id.replace(source_field_id, target_field.id)
                    return res_id

                # 7.5. Commit
                if not dry_run:
                    new_translations = [DatabaseTranslation(
                        id=update_identifier(t.id),
                        original=t.original,
                        translated=t.translated,
                        autoTranslated=t.auto_translated
                    ) for t in existing_translations.translated_strings]
                    
                    client.api.update_form_translations(
                        form.id, language_code,
                        UpdateDatabaseTranslationsDTO(strings=new_translations)
                    )

            progress.advance(task)

    if dry_run:
        console.print("[bold cyan]Dry run completed successfully. No changes were made.[/bold cyan]")
    else:
        console.print("[bold green]Transfer executed successfully.[/bold green]")


if __name__ == "__main__":
    app()
