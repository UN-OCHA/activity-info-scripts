import csv
from typing import Annotated, Optional

import pandas as pd
import typer
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from activityinfo_client_helpers import find_resource_by_prefix, get_form_records_with_multiref
from activity_info_api_client.api.default import (
    get_database_tree,
    get_database_translations,
    get_form_schema,
    get_form_translations,
    update_database,
    update_database_translations,
    update_form_records,
    update_form_schema_translations,
)
from activity_info_api_client.models import (
    DatabaseResourceType,
    RecordUpdateChange,
    RecordUpdateChangeFields,
    RecordUpdateRequest,
    TranslationString,
    UpdateDatabaseRequest,
    UpdateTranslationsRequest,
)
from activity_info_api_client.types import UNSET

from api_client_utils import get_client as get_generated_client, call_api, console

# Initialize a Typer sub-application for translation management
app = typer.Typer(no_args_is_help=True)


def _api_request(client, endpoint, **kwargs):
    result, api_error = call_api(
        endpoint.sync_detailed,
        client=client,
        exit_on_error=False,
        **kwargs,
    )
    if api_error:
        raise typer.Exit(code=1)
    return result


@app.command(help="Migrate existing translations from a source database to a target database.")
def transfer(
        source_database_id: Annotated[str, typer.Argument(help="The ActivityInfo ID of the origin database")],
        target_database_id: Annotated[str, typer.Argument(help="The ActivityInfo ID of the target database")],
        language_code: Annotated[str, typer.Argument(help="The two-letter ISO language code (e.g., 'fr', 'es')")],
        dry_run: Annotated[bool, typer.Option(help="Do not actually perform the transfer")] = False,
):
    """
    Synchronize translations for a specific language between two ActivityInfo databases.
    """
    client = get_generated_client()

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
                  BarColumn(),
                  TaskProgressColumn(),
                  console=console) as progress:

        # --- 1. Initialization & Data Retrieval ---
        task = progress.add_task("Fetching database structures...", total=None)

        source_tree = _api_request(client, get_database_tree, database_id=source_database_id)
        target_tree = _api_request(client, get_database_tree, database_id=target_database_id)
        source_db_translations = _api_request(
            client, get_database_translations, database_id=source_database_id, language_code=language_code
        )

        # --- 2. Validation ---
        if target_tree.original_language is not UNSET and target_tree.original_language.lower() != "en":
            console.print(
                f"[bold red]Error:[/bold red] Target DB original language must be 'en', found '{target_tree.original_language}'")
            raise typer.Exit(code=1)

        # --- 3. Build Resource ID Mapping ---
        resource_id_map = {source_database_id: target_database_id}
        target_resources_by_key = {(res.type_, res.label): res for res in target_tree.resources}

        for s_res in source_tree.resources:
            key = (s_res.type_, s_res.label)
            if key in target_resources_by_key:
                resource_id_map[s_res.id] = target_resources_by_key[key].id

        def map_identifier(res_id: str, field_id_map: Optional[dict[str, str]] = None) -> str:
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
        target_languages = [] if target_tree.languages is UNSET else target_tree.languages
        if language_code not in target_languages:
            progress.update(task, description=f"Adding '{language_code}' to target database...")
            if not dry_run:
                _api_request(client, update_database, database_id=target_database_id, body=UpdateDatabaseRequest(
                    language_updates=[language_code], original_language="en"
                ))

        # --- 5. Sync Database-Level Translations ---
        progress.update(task, description="Syncing database-level translations...")
        if not dry_run:
            mapped_db_strings = [
                TranslationString(
                    id=map_identifier(t.id),
                    original=t.original,
                    translated=t.translated,
                    auto_translated=t.auto_translated
                ) for t in source_db_translations.translated_strings
            ]
            _api_request(client, update_database_translations, database_id=target_database_id,
                    language_code=language_code, body=UpdateTranslationsRequest(strings=mapped_db_strings))

        # --- 6. Sync Form-Level Translations ---
        target_forms = [res for res in target_tree.resources if res.type_ == DatabaseResourceType.FORM]
        progress.update(task, description="Syncing form-level translations...", total=len(target_forms))

        for form in target_forms:
            source_form = next((res for res in source_tree.resources if
                                res.label == form.label and res.type_ == DatabaseResourceType.FORM), None)
            if not source_form:
                progress.advance(task)
                continue

            source_translations = _api_request(client, get_form_translations, database_id=source_database_id,
                                          form_id=source_form.id, language_code=language_code)
            source_schema = _api_request(client, get_form_schema, form_id=source_form.id)
            target_schema = _api_request(client, get_form_schema, form_id=form.id)

            target_fields_by_label = {f.label: f for f in target_schema.elements}
            field_id_map = {}
            for s_field in source_schema.elements:
                if s_field.label in target_fields_by_label:
                    field_id_map[s_field.id] = target_fields_by_label[s_field.label].id

            if not dry_run:
                new_translations = [
                    TranslationString(
                        id=map_identifier(t.id, field_id_map),
                        original=t.original,
                        translated=t.translated,
                        auto_translated=t.auto_translated
                    ) for t in source_translations.translated_strings
                ]
                _api_request(client, update_form_schema_translations, form_id=form.id, language_code=language_code,
                        body=UpdateTranslationsRequest(strings=new_translations))
            progress.advance(task)

    console.print(f"[bold green]Translation sync for '{language_code}' completed.[/bold green]")


@app.command(help="Upsert translations from an Excel file to an ActivityInfo database.", no_args_is_help=True)
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
    client = get_generated_client()
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
                  BarColumn(),
                  TaskProgressColumn(),
                  console=console) as progress:
        task = progress.add_task("Fetching database structure...", total=None)

        target_tree = _api_request(client, get_database_tree, database_id=target_database_id)

        if target_tree.original_language is not UNSET and target_tree.original_language.lower() != "en":
            console.print(
                f"[bold red]Error:[/bold red] Target DB original language must be 'en', found '{target_tree.original_language}'")
            raise typer.Exit(code=1)

        # --- Language Setup ---
        target_languages = [] if target_tree.languages is UNSET else target_tree.languages
        if language_code not in target_languages:
            progress.update(task, description=f"Adding '{language_code}' to target database...")
            _api_request(client, update_database, database_id=target_database_id, body=UpdateDatabaseRequest(
                language_updates=[language_code], original_language="en"
            ))

        schema_missing_en = set()  # Set of (Context, SubContext, StringContext, Original)

        # --- SCHEMA TRANSLATIONS ---
        if not skip_schema:
            progress.update(task, description="Processing schema translations...")
            try:
                df_schema = pd.read_excel(source_translations_file, sheet_name='CM - Schema', header=1)
                df_metrics = pd.read_excel(source_translations_file, sheet_name='CM - Schema (Metrics)', header=0)
            except ValueError as e:
                console.print(f"[bold red]Error:[/bold red] Required schema sheet missing from Excel file: {e}")
                raise typer.Exit(code=1)

            target_lang_col = language_code.upper()
            if target_lang_col not in df_schema.columns or target_lang_col not in df_metrics.columns:
                console.print(f"[bold red]Error:[/bold red] '{target_lang_col}' column missing from schema sheets.")
                raise typer.Exit(code=1)

            schema_map_main = df_schema.dropna(subset=['EN', target_lang_col]).set_index('EN')[
                target_lang_col].to_dict()
            schema_map_metrics = df_metrics.dropna(subset=['EN', target_lang_col]).set_index('EN')[
                target_lang_col].to_dict()
            schema_map = {str(k).strip(): str(v).strip() for k, v in schema_map_main.items()}
            schema_map.update({str(k).strip(): str(v).strip() for k, v in schema_map_metrics.items()})

            console.print(f"[dim]Loaded {len(schema_map)} unique schema translation mappings.[/dim]")

            # Database-level translations
            progress.update(task, description="Syncing database-level translations...")
            db_strings = [
                TranslationString(id=f"resource:{target_database_id}:label", original=target_tree.label,
                                  translated="", auto_translated=False)
            ]
            if target_tree.description is not UNSET and target_tree.description:
                db_strings.append(TranslationString(id=f"resource:{target_database_id}:description",
                                                    original=target_tree.description, translated="",
                                                    auto_translated=False))
            for role in target_tree.roles:
                db_strings.append(TranslationString(id=f"role:{role.id}:label", original=role.label, translated="",
                                                    auto_translated=False))

            updated_db_strings = []
            for t in db_strings:
                if t.original.strip() in schema_map:
                    t.translated = schema_map[t.original.strip()]
                    updated_db_strings.append(t)
                else:
                    translation_id_components = t.id.split(":")
                    string_context = translation_id_components[0] + ":" + translation_id_components[2]
                    schema_missing_en.add(("Database", "", string_context, t.original))

            if updated_db_strings:
                console.print(f"[dim]Updating {len(updated_db_strings)} database-level strings...[/dim]")
                _api_request(client, update_database_translations, database_id=target_database_id, language_code=language_code,
                        body=UpdateTranslationsRequest(strings=updated_db_strings))

            # Form-level translations
            target_forms = [res for res in target_tree.resources if res.type_ == DatabaseResourceType.FORM]
            target_folders = [res for res in target_tree.resources if res.type_ == DatabaseResourceType.FOLDER]

            def get_translation(original_label: str) -> Optional[str]:
                # 1. Try exact match
                cleaned_label = original_label.strip()
                if cleaned_label in schema_map:
                    return schema_map[cleaned_label]

                # 2. Try prefix-based match (e.g., "0.3.3_Metric_Configuration" matches "0.3.3" in map)
                # We only do this for labels that look like they have a prefix (start with numbers and dots)
                if "_" in cleaned_label:
                    prefix = cleaned_label.split("_")[0]
                    if prefix in schema_map:
                        return schema_map[prefix]

                return None

            for folder in progress.track(target_folders, description="Syncing folder schema translations"):
                trans_val = get_translation(folder.label)
                if trans_val:
                    console.print(f"[dim]Updating folder '{folder.label}' -> '{trans_val}'...[/dim]")
                    _api_request(client, update_database_translations, database_id=target_database_id,
                            language_code=language_code,
                            body=UpdateTranslationsRequest(strings=[
                                TranslationString(id=f"resource:{folder.id}:label", original=folder.label,
                                                  translated=trans_val, auto_translated=False)
                            ]))
                else:
                    schema_missing_en.add(("Database", "Folder", "resource:label", folder.label))

            for form in progress.track(target_forms, description="Syncing form schema translations"):
                schema = _api_request(client, get_form_schema, form_id=form.id)

                form_trans_val = get_translation(form.label)
                form_strings = [TranslationString(id=f"resource:{form.id}:label", original=form.label,
                                                  translated=form_trans_val or "", auto_translated=False)]
                if not form_trans_val:
                    schema_missing_en.add((form.label, "", "resource:label", form.label))

                for field in schema.elements:
                    field_code = "" if field.code is UNSET else field.code
                    field_trans_val = get_translation(field.label)
                    form_strings.append(TranslationString(id=f"field:{field.id}:label", original=field.label,
                                                          translated=field_trans_val or "", auto_translated=False))
                    if not field_trans_val:
                        schema_missing_en.add((form.label, field_code, "field:label", field.label))

                    if field.description is not UNSET and field.description:
                        desc_trans_val = get_translation(field.description)
                        form_strings.append(
                            TranslationString(id=f"field:{field.id}:description", original=field.description,
                                              translated=desc_trans_val or "", auto_translated=False))
                        if not desc_trans_val:
                            schema_missing_en.add((form.label, field_code, "field:description", field.description))

                    if field.type_parameters is not UNSET:
                        lookup_configs = field.type_parameters.lookup_configs
                        if lookup_configs is not UNSET:
                            for lookup_config in lookup_configs:
                                lookup_label = lookup_config.lookup_label
                                if lookup_label is UNSET:
                                    continue
                                lookup_trans_val = get_translation(lookup_label)
                                form_strings.append(
                                    TranslationString(id=f"lookup:{lookup_config.id}:label",
                                                      original=lookup_label,
                                                      translated=lookup_trans_val or "", auto_translated=False))
                                if not lookup_trans_val:
                                    schema_missing_en.add((form.label, field_code, "lookup:label", lookup_label))

                        values = field.type_parameters.values
                        if values is not UNSET:
                            for value in values:
                                value_trans_val = get_translation(value.label)
                                form_strings.append(
                                    TranslationString(id=f"item:{value.id}:label", original=value.label,
                                                      translated=value_trans_val or "", auto_translated=False))
                                if not value_trans_val:
                                    schema_missing_en.add((form.label, field_code, "item:label", value.label))


                updated_form_strings = [t for t in form_strings if t.translated]

                if updated_form_strings:
                    console.print(
                        f"[dim]Updating {len(updated_form_strings)} strings for form '{form.label}'...[/dim]")
                    _api_request(client, update_form_schema_translations, form_id=form.id, language_code=language_code,
                            body=UpdateTranslationsRequest(strings=updated_form_strings))

            console.print(f"[bold green]Schema translations completed.[/bold green]")
            if schema_output and schema_missing_en:
                schema_missing_en_cleaned = {
                    tuple("" if x is None or x is UNSET else x for x in item)
                    for item in schema_missing_en
                }
                with open(schema_output, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(['Context', 'Subcontext', 'String Context', 'EN'])
                    for context, subContext, strContext, val in sorted(schema_missing_en_cleaned):
                        writer.writerow([context, subContext, strContext, val])

        # --- REFERENCE VALUE TRANSLATIONS ---
        if not skip_reference:
            progress.update(task, description="Processing reference value translations...")
            try:
                df_values = pd.read_excel(source_translations_file, sheet_name='CM - Values', header=1)
            except ValueError:
                console.print("[bold red]Error:[/bold red] 'CM - Values' sheet missing.")
                raise typer.Exit(code=1)

            target_lang_col = language_code.upper()
            target_field_suffix = f"_{target_lang_col}"
            not_found_items = []
            df_values['Form System Prefix'] = df_values['Form System Prefix'].astype(str)

            for prefix, group in progress.track(df_values.groupby('Form System Prefix'),
                                                description="Syncing reference values"):
                # Robust form matching: Try exact match, then prefix with underscore or dot, then just prefix
                form_res = find_resource_by_prefix(
                    [res for res in target_tree.resources if res.type_ == DatabaseResourceType.FORM],
                    str(prefix)
                )

                if not form_res:
                    for _, row in group.iterrows(): not_found_items.append(
                        {'Form System Prefix': prefix, 'Field Code': row['Field Code'], 'Refcode': row['Refcode'],
                         'Reason': 'Form not found'})
                    continue

                console.print(f"[dim]Processing form '{form_res.label}' (Prefix: {prefix})...[/dim]")
                schema = _api_request(client, get_form_schema, form_id=form_res.id)
                records = get_form_records_with_multiref(
                    client, form_res.id, request_api=_api_request
                )
                field_code_to_id = {
                    field.code: field.id for field in schema.elements if field.code is not UNSET
                }

                record_map = {}
                for rec in records:
                    for key in ['CCODE', 'REFCODE', 'REFCODE_MAN']:
                        value = rec.get(key)
                        if value is not None:
                            value_string = str(value).strip()
                            if value_string and value_string.lower() != "none":
                                record_map[value_string] = rec

                updates = []
                for _, row in group.iterrows():
                    field_code, refcode = str(row['Field Code']).strip(), str(row['Refcode']).strip()
                    target_field_code = f"{field_code}{target_field_suffix}"

                    if target_field_code not in field_code_to_id:
                        continue

                    record = record_map.get(refcode)
                    if not record:
                        not_found_items.append(
                            {'Form System Prefix': prefix, 'Field Code': field_code, 'Refcode': refcode,
                             'Reason': f'Record {refcode} not found'})
                        continue

                    translation = row.get(target_lang_col)
                    if not pd.isna(translation) and str(translation).strip() != "":
                        if replace_translations or not record.get(target_field_code):
                            updates.append(RecordUpdateChange(
                                form_id=form_res.id,
                                record_id=record['@id'],
                                fields=RecordUpdateChangeFields.from_dict({
                                    field_code_to_id[target_field_code]: str(translation).strip()
                                }),
                            ))

                if updates:
                    console.print(f"[dim]Pushing {len(updates)} record updates for '{form_res.label}'...[/dim]")
                    for start in range(0, len(updates), 100):
                        _api_request(client, update_form_records,
                                     body=RecordUpdateRequest(changes=updates[start:start + 100]))

            console.print("[bold green]Reference value translations completed.[/bold green]")
            if reference_output and not_found_items:
                with open(reference_output, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=['Form System Prefix', 'Field Code', 'Refcode', 'Reason'])
                    writer.writeheader()
                    writer.writerows(not_found_items)

    console.print("[bold green]Upsert executed successfully.[/bold green]")


if __name__ == "__main__":
    app()
