from itertools import groupby
from operator import itemgetter
from typing import Annotated, Optional, List, Dict

import typer
from cuid2 import Cuid
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from api.models import SchemaFieldDTO, FieldTypeParametersUpdateDTO, FieldType, TypeParameterLookupConfig
from common import filter_data_forms
from utils import get_client, console, handle_api_errors

app = typer.Typer(no_args_is_help=True)

METRIC_CONFIG_FORM_PREFIX = "0.3.3"
DISAGG_CONFIG_FORM_PREFIX = "0.3.2"


def strip_metric_prefix(code: str) -> str:
    if code.startswith("AMOUNT_"):
        return code[len("AMOUNT_"):]
    if code.startswith("METRIC_"):
        return code[len("METRIC_"):]
    return code


def is_metric_field(code: str) -> bool:
    return code.startswith("AMOUNT_") or code.startswith("METRIC_")


def is_disag_field(code: str) -> bool:
    return code.startswith("DISAG_")


def get_metric_base_code(code: str) -> str:
    stripped = strip_metric_prefix(code)
    for suffix in ["_MAN", "_ECALC", "_ICALC"]:
        if stripped.endswith(suffix):
            return stripped[:-len(suffix)]
    return stripped


@app.command(help="Adjust metric fields in data forms", no_args_is_help=True)
def metric(target_database_id: Annotated[str, typer.Argument(help="The ID of the target database")],
           root_folder_id: Annotated[
               Optional[str], typer.Argument(help="The root folder ID of the data folders (optional)")] = None,
           remove_fields: Annotated[bool, typer.Option(help="Remove existing fields missing from the config")] = False,
           rebuild_fields: Annotated[bool, typer.Option(help="Rebuild existing fields from the config")] = False):
    client = get_client()
    cuid = Cuid(length=18)

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
                  BarColumn(),
                  TaskProgressColumn(),
                  console=console) as progress:
        # 1. Initialize Task
        task = progress.add_task("Fetching database configuration...", total=None)

        # 2. Get the target DB's tree
        with handle_api_errors(f"Could not get tree for {target_database_id}"):
            target_tree = client.api.get_database_tree(target_database_id)

        # 3. Filter data forms
        # First, find forms in folders starting with 3, 4, 5, 6
        folder_forms = filter_data_forms(target_tree, root_folder_id or target_database_id)

        # Second, find forms mentioned in 0.1.2 config form
        config_form_012 = next((res for res in target_tree.resources if res.label.startswith("0.1.2")), None)
        config_form_names = set()
        if config_form_012:
            with handle_api_errors("Could not obtain 0.1.2 config form records"):
                config_records = client.api.get_form(config_form_012.id)
                for rec in config_records:
                    name = rec.get("SYSNAME") or rec.get("SYS_NAME")
                    if name:
                        config_form_names.add(name)

        # Combine them
        data_forms = [res for res in target_tree.resources if res.label in config_form_names]
        # Add folder forms if not already present
        existing_ids = {res.id for res in data_forms}
        for res in folder_forms:
            if res.id not in existing_ids:
                data_forms.append(res)

        data_forms_by_name = {res.label: res for res in data_forms}

        # 4. Get metric config form records
        metric_config_form = next(
            (res for res in target_tree.resources if res.label.startswith(METRIC_CONFIG_FORM_PREFIX)), None)
        if not metric_config_form:
            progress.stop()
            console.print(
                f"[bold red]Error:[/bold red] Could not find metric configuration form starting with {METRIC_CONFIG_FORM_PREFIX}")
            raise typer.Exit(code=1)

        progress.update(task, description=f"Fetching records from {metric_config_form.label}...")
        with handle_api_errors(f"Could not get records for {metric_config_form.id}"):
            records = client.api.get_form(metric_config_form.id)

        # Sort the records by DFORM.SYSNAME and then REFORDER
        records.sort(key=lambda r: (r.get("DFORM.SYSNAME") or "", r.get("REFORDER") or ""))

        # Group records by DFORM.SYSNAME to get the list of forms to process
        form_records_by_sysname = {
            sysname: list(items)
            for sysname, items in groupby(records, key=lambda r: r.get("DFORM.SYSNAME") or r.get("DFORM_SYSNAME") or r.get("SYSNAME"))
            if sysname
        }

        # 5. Process forms
        # We iterate over ALL data forms found in the database (or folder)
        # so we can handle removals even for forms that no longer have 0.3.3 records.
        progress.update(task, description="Adjusting metric fields...", total=len(data_forms_by_name))

        for sysname, target_form_res in data_forms_by_name.items():
            progress.update(task, description=f"Processing form: {sysname}")

            form_records = form_records_by_sysname.get(sysname, [])

            # If there are no records in 0.3.3 for this form, 
            # and we are NOT removing fields, we can skip it.
            if not form_records and not remove_fields:
                progress.advance(task)
                continue

            with handle_api_errors(f"Could not process schema for {sysname}"):
                schema = client.api.get_form_schema(target_form_res.id)

                # Identify metric insertion point and existing metric fields
                insertion_index = 0

                # Find the point after ID fields (PROJ, IND, CSL, CST) and SEG_, DISAG_
                for i, element in enumerate(schema.elements):
                    code = element.code
                    if code in ["PROJ", "IND", "CSL", "CST"] or code.startswith("SEG_") or code.startswith("DISAG_"):
                        insertion_index = i + 1

                # Separate elements
                non_metric_before = schema.elements[:insertion_index]
                rest_elements = schema.elements[insertion_index:]

                other_elements = [e for e in rest_elements if not is_metric_field(e.code)]
                existing_metric_elements = [e for e in rest_elements if is_metric_field(e.code)]

                # Maps base_code -> list of elements in its total-schema
                metric_schemas: Dict[str, List[SchemaFieldDTO]] = {}
                for e in existing_metric_elements:
                    base_code = get_metric_base_code(e.code)
                    if base_code not in metric_schemas:
                        metric_schemas[base_code] = []
                    metric_schemas[base_code].append(e)

                final_metric_base_codes = []
                processed_base_codes = set()

                # Process records from 0.3.3 for this form
                for record in form_records:
                    # Support both dotted and underscored names for robustness
                    refcode_man_val = record.get("REFCODE_MAN") or record.get("REFCODE")
                    if not refcode_man_val:
                        continue
                    
                    base_code = strip_metric_prefix(refcode_man_val)
                    if base_code in processed_base_codes:
                        continue
                        
                    final_metric_base_codes.append(base_code)
                    processed_base_codes.add(base_code)

                    display_refcode = record.get("DISPLAY.REFCODE") or record.get("DISPLAY_REFCODE") or record.get("DISPLAY")
                    name = record.get("NAME")
                    ccode = record.get("CCODE")
                    eform_refcode = record.get("DFORM.EFORM.REFCODE") or record.get("DFORM_EFORM_REFCODE") or record.get("EFORM_REFCODE") or record.get("EFORM")

                    # Decide if we create or update
                    needs_rebuild = rebuild_fields
                    existing_schema_elements = metric_schemas.get(base_code)

                    if existing_schema_elements and not needs_rebuild:
                        continue

                    # Create new total-schema or rebuild
                    new_elements = []

                    # Element IDs
                    ids = [cuid.generate() for _ in range(4)]
                    if existing_schema_elements:
                        # Try to preserve IDs if rebuilding
                        for i, suffix in enumerate(["_MAN", "_ECALC", "_ICALC", ""]):
                            full_code = f"AMOUNT_{base_code}{suffix}" if suffix else f"AMOUNT_{base_code}"
                            alt_code = f"METRIC_{base_code}{suffix}" if suffix else f"METRIC_{base_code}"

                            found = next((e for e in existing_schema_elements if e.code == full_code or e.code == alt_code),
                                         None)
                            if found:
                                ids[i] = found.id

                    relevance_man = f'!ISBLANK(SEARCH("|{ccode}|", CONCAT("|", TEXTJOIN("|", TRUE, {eform_refcode}.MM.METRICS_MAN.CCODE), "|")))'
                    relevance_others = f'!ISBLANK(SEARCH("|{ccode}|", CONCAT("|", TEXTJOIN("|", TRUE, {eform_refcode}.MM.METRICS_MAN.CCODE), "|"))) || !ISBLANK(SEARCH("|{ccode}|", CONCAT("|", TEXTJOIN("|", TRUE, {eform_refcode}.MM.METRICS_CALC.CCODE), "|")))'

                    # 1. MAN element
                    if display_refcode == "MAN":
                        new_elements.append(SchemaFieldDTO(
                            id=ids[0],
                            code=f"AMOUNT_{base_code}_MAN",
                            label=f"{name} (Manual)",
                            required=False,
                            type=FieldType.quantity,
                            relevanceCondition=relevance_man,
                            typeParameters=FieldTypeParametersUpdateDTO(units="", aggregation="SUM")
                        ))

                    # 2. ECALC element
                    new_elements.append(SchemaFieldDTO(
                        id=ids[1],
                        code=f"AMOUNT_{base_code}_ECALC",
                        label=f"{name} (External Calc)",
                        required=False,
                        dataEntryVisible=False,
                        tableVisible=False,
                        type=FieldType.quantity,
                        relevanceCondition=relevance_others,
                        typeParameters=FieldTypeParametersUpdateDTO(units="", aggregation="SUM")
                    ))

                    # 3. ICALC element
                    new_elements.append(SchemaFieldDTO(
                        id=ids[2],
                        code=f"AMOUNT_{base_code}_ICALC",
                        label=f"{name} (Internal Calc)",
                        required=False,
                        dataEntryVisible=False,
                        tableVisible=False,
                        type=FieldType.calculated,
                        relevanceCondition=relevance_others,
                        typeParameters=FieldTypeParametersUpdateDTO(formula="VALUE(\"#\")")
                    ))

                    # 4. Final element
                    formula = f"COALESCE({ids[1]}, {ids[2]})"
                    if display_refcode == "MAN":
                        formula = f"COALESCE({ids[0]}, {ids[1]}, {ids[2]})"

                    new_elements.append(SchemaFieldDTO(
                        id=ids[3],
                        code=f"AMOUNT_{base_code}",
                        label=name,
                        required=False,
                        type=FieldType.calculated,
                        relevanceCondition=relevance_others,
                        typeParameters=FieldTypeParametersUpdateDTO(formula=formula)
                    ))

                    metric_schemas[base_code] = new_elements

                # Handle fields not in 0.3.3
                remaining_base_codes = [bc for bc in metric_schemas.keys() if bc not in processed_base_codes]
                if remove_fields:
                    for bc in remaining_base_codes:
                        if bc in metric_schemas:
                            del metric_schemas[bc]
                else:
                    for bc in remaining_base_codes:
                        final_metric_base_codes.append(bc)

                # Construct final elements list
                final_metric_elements = []
                for bc in final_metric_base_codes:
                    if bc in metric_schemas:
                        final_metric_elements.extend(metric_schemas[bc])

                schema.elements = non_metric_before + final_metric_elements + other_elements

                # Update schema
                client.api.update_form_schema(schema)

            progress.advance(task)

    console.print("[bold green]Success:[/bold green] Metric fields adjusted.")


@app.command(help="Adjust disaggregation fields in data forms", no_args_is_help=True)
def disagg(target_database_id: Annotated[str, typer.Argument(help="The ID of the target database")],
           root_folder_id: Annotated[
               Optional[str], typer.Argument(help="The root folder ID of the data folders (optional)")] = None,
           remove_fields: Annotated[bool, typer.Option(help="Remove existing fields missing from the config")] = False,
           rebuild_fields: Annotated[bool, typer.Option(help="Rebuild existing fields from the config")] = False):
    client = get_client()
    cuid = Cuid(length=18)

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
                  BarColumn(),
                  TaskProgressColumn(),
                  console=console) as progress:
        # 1. Initialize Task
        task = progress.add_task("Fetching database configuration...", total=None)

        # 2. Get the target DB's tree
        with handle_api_errors(f"Could not get tree for {target_database_id}"):
            target_tree = client.api.get_database_tree(target_database_id)

        # 3. Filter data forms
        # First, find forms in folders starting with 3, 4, 5, 6
        folder_forms = filter_data_forms(target_tree, root_folder_id or target_database_id)

        # Second, find forms mentioned in 0.1.2 config form
        config_form_012 = next((res for res in target_tree.resources if res.label.startswith("0.1.2")), None)
        config_form_names = set()
        if config_form_012:
            with handle_api_errors("Could not obtain 0.1.2 config form records"):
                config_records = client.api.get_form(config_form_012.id)
                for rec in config_records:
                    name = rec.get("SYSNAME") or rec.get("SYS_NAME")
                    if name:
                        config_form_names.add(name)

        # Combine them
        data_forms = [res for res in target_tree.resources if res.label in config_form_names]
        # Add folder forms if not already present
        existing_ids = {res.id for res in data_forms}
        for res in folder_forms:
            if res.id not in existing_ids:
                data_forms.append(res)

        data_forms_by_name = {res.label: res for res in data_forms}

        # Resources by name for RFORM.SYSNAME lookups (all forms in database)
        all_forms_by_name = {res.label: res for res in target_tree.resources if res.type == "FORM"}

        # 4. Get disagg config form records
        disagg_config_form = next(
            (res for res in target_tree.resources if res.label.startswith(DISAGG_CONFIG_FORM_PREFIX)), None)
        if not disagg_config_form:
            progress.stop()
            console.print(
                f"[bold red]Error:[/bold red] Could not find disaggregation configuration form starting with {DISAGG_CONFIG_FORM_PREFIX}")
            raise typer.Exit(code=1)

        progress.update(task, description=f"Fetching records from {disagg_config_form.label}...")
        with handle_api_errors(f"Could not get records for {disagg_config_form.id}"):
            records = client.api.get_form(disagg_config_form.id)

        # Sort the records by DFORM.SYSNAME and then REFORDER
        records.sort(key=lambda r: (r.get("DFORM.SYSNAME") or "", r.get("REFORDER") or ""))

        # Group records by DFORM.SYSNAME to get the list of forms to process
        form_records_by_sysname = {
            sysname: list(items)
            for sysname, items in groupby(records, key=lambda r: r.get("DFORM.SYSNAME") or r.get("DFORM_SYSNAME") or r.get("SYSNAME"))
            if sysname
        }

        # 5. Process forms
        progress.update(task, description="Adjusting disaggregation fields...", total=len(data_forms_by_name))

        for sysname, target_form_res in data_forms_by_name.items():
            progress.update(task, description=f"Processing form: {sysname}")

            form_records = form_records_by_sysname.get(sysname, [])

            if not form_records and not remove_fields:
                progress.advance(task)
                continue

            with handle_api_errors(f"Could not process schema for {sysname}"):
                schema = client.api.get_form_schema(target_form_res.id)

                # Identify insertion point
                # After: PROJ, IND, CSL, CST and SEG_
                # Before: AMOUNT_ or METRIC_
                insertion_index = 0
                first_metric_index = len(schema.elements)

                for i, element in enumerate(schema.elements):
                    code = element.code
                    if code in ["PROJ", "IND", "CSL", "CST"] or code.startswith("SEG_"):
                        insertion_index = i + 1
                    if is_metric_field(code) and i < first_metric_index:
                        first_metric_index = i

                # Separate elements
                elements_before = schema.elements[:insertion_index]
                rest_elements = schema.elements[insertion_index:]

                # Existing DISAG_ fields are between insertion_index and first_metric_index
                # But we can just find all DISAG_ in rest_elements for now.
                disag_elements = [e for e in rest_elements if is_disag_field(e.code)]
                other_elements = [e for e in rest_elements if not is_disag_field(e.code)]

                existing_disags_by_code = {e.code: e for e in disag_elements}

                final_disag_codes = []
                processed_codes = set()

                # Process records from 0.3.2
                for record in form_records:
                    ref_code = record.get("REFCODE")
                    if not ref_code:
                        continue

                    if ref_code in processed_codes:
                        continue

                    final_disag_codes.append(ref_code)
                    processed_codes.add(ref_code)

                    name = record.get("NAME")
                    ccode = record.get("CCODE")
                    eform_refcode = record.get("DFORM.EFORM.REFCODE") or record.get("DFORM_EFORM_REFCODE") or record.get("EFORM_REFCODE") or record.get("EFORM")
                    optmand = record.get("OPTMAND")
                    rform_sysname = record.get("RFORM.SYSNAME") or record.get("RFORM_SYSNAME") or record.get("RFORM")

                    # Decide if we create or update
                    needs_rebuild = rebuild_fields
                    existing_element = existing_disags_by_code.get(ref_code)

                    if existing_element and not needs_rebuild:
                        continue

                    # Create or Rebuild
                    field_id = existing_element.id if existing_element else cuid.generate()

                    # Lookup RFORM.SYSNAME
                    ref_form = all_forms_by_name.get(rform_sysname)
                    if not ref_form:
                        console.print(
                            f"[yellow]Warning:[/yellow] Reference form {rform_sysname} not found for field {ref_code}. Skipping.")
                        continue

                    relevance = f'!ISBLANK(SEARCH("|{ref_code}|", CONCAT("|", TEXTJOIN("|", TRUE, {eform_refcode}.DM.RFORMS_OPT.REFCODE), "|"))) || !ISBLANK(SEARCH("|{ccode}|", CONCAT("|", TEXTJOIN("|", TRUE, {eform_refcode}.DM.DISAGCONFIGS_OPT.CCODE), "|")))'

                    new_element = SchemaFieldDTO(
                        id=field_id,
                        code=ref_code,
                        label=name,
                        required=True, # Key fields MUST be required in ActivityInfo
                        type=FieldType.reference,
                        key=True,
                        relevanceCondition=relevance,
                        validationCondition="",
                        typeParameters=FieldTypeParametersUpdateDTO(
                            cardinality="single",
                            range=[{"formId": ref_form.id}],
                            lookupConfigs=[
                                TypeParameterLookupConfig(
                                    id=cuid.generate(),
                                    formula="REFLABEL",
                                    lookupLabel="Reference Label"
                                )
                            ]
                        )
                    )
                    existing_disags_by_code[ref_code] = new_element

                # Handle fields not in 0.3.2
                remaining_codes = [c for c in existing_disags_by_code.keys() if c not in processed_codes]
                if remove_fields and remaining_codes:
                    # Fetch records to check for data in fields being removed
                    with handle_api_errors(f"Could not fetch records for {sysname} to check for field removal"):
                        records_to_check = client.api.get_form(target_form_res.id)
                        
                    records_to_delete = []
                    for rec in records_to_check:
                        should_delete = False
                        for c in remaining_codes:
                            # Check for field code, field code with .@id (for references),
                            # or field ID if it matches
                            existing_field = existing_disags_by_code.get(c)
                            field_id = existing_field.id if existing_field else None
                            
                            if rec.get(c) or rec.get(f"{c}.@id") or (field_id and (rec.get(field_id) or rec.get(f"{field_id}.@id"))):
                                should_delete = True
                                break
                        if should_delete:
                            records_to_delete.append(rec["@id"])
                    
                    if records_to_delete:
                        console.print(f"[yellow]Deleting {len(records_to_delete)} records from {sysname} to allow field removal...[/yellow]")
                        from api.models import RecordUpdateDTO
                        client.api.update_form_records([
                            RecordUpdateDTO(formId=target_form_res.id, recordId=rid, deleted=True, fields={})
                            for rid in records_to_delete
                        ])

                    for c in remaining_codes:
                        if c in existing_disags_by_code:
                            del existing_disags_by_code[c]
                else:
                    for c in remaining_codes:
                        final_disag_codes.append(c)

                # Construct final elements list
                final_ordered_disags = []
                for c in final_disag_codes:
                    if c in existing_disags_by_code:
                        final_ordered_disags.append(existing_disags_by_code[c])

                schema.elements = elements_before + final_ordered_disags + other_elements

                # Update schema
                client.api.update_form_schema(schema)

            progress.advance(task)

    console.print("[bold green]Success:[/bold green] Disaggregation fields adjusted.")


if __name__ == "__main__":
    app()
