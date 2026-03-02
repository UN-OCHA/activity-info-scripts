from itertools import groupby
from typing import Annotated, Optional, List, Dict, Any, Tuple

import typer
from cuid2 import Cuid
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from api.models import SchemaFieldDTO, FieldTypeParametersUpdateDTO, FieldType, TypeParameterLookupConfig, \
    FormSchema
from common import filter_data_forms, get_records_with_multiref
from utils import get_client, console, handle_api_errors

app = typer.Typer(no_args_is_help=True)

METRIC_CONFIG_FORM_PREFIX = "0.3.3"
DISAGG_CONFIG_FORM_PREFIX = "0.3.2"
SEG_CONFIG_FORM_PREFIX = "0.3.1"
DATA_CONFIG_FORM_012 = "0.1.2"
ENTITY_CONFIG_FORM_011 = "0.1.1"


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
            for sysname, items in
            groupby(records, key=lambda r: r.get("DFORM.SYSNAME") or r.get("DFORM_SYSNAME") or r.get("SYSNAME"))
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

                    display_refcode = record.get("DISPLAY.REFCODE") or record.get("DISPLAY_REFCODE") or record.get(
                        "DISPLAY")
                    name = record.get("NAME")
                    ccode = record.get("CCODE")
                    eform_refcode = record.get("DFORM.EFORM.REFCODE") or record.get(
                        "DFORM_EFORM_REFCODE") or record.get("EFORM_REFCODE") or record.get("EFORM")

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

                            found = next(
                                (e for e in existing_schema_elements if e.code == full_code or e.code == alt_code),
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
            for sysname, items in
            groupby(records, key=lambda r: r.get("DFORM.SYSNAME") or r.get("DFORM_SYSNAME") or r.get("SYSNAME"))
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
                    eform_refcode = record.get("DFORM.EFORM.REFCODE") or record.get(
                        "DFORM_EFORM_REFCODE") or record.get("EFORM_REFCODE") or record.get("EFORM")
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
                        required=True,  # Key fields MUST be required in ActivityInfo
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

                            if rec.get(c) or rec.get(f"{c}.@id") or (
                                    field_id and (rec.get(field_id) or rec.get(f"{field_id}.@id"))):
                                should_delete = True
                                break
                        if should_delete:
                            records_to_delete.append(rec["@id"])

                    if records_to_delete:
                        console.print(
                            f"[yellow]Deleting {len(records_to_delete)} records from {sysname} to allow field removal...[/yellow]")
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


@app.command(help="Adjust segmentation fields in CDE, LFE, IND/CST/CSL and data forms", no_args_is_help=True)
def segment(target_database_id: Annotated[str, typer.Argument(help="The ID of the target database")],
            remove_fields: Annotated[bool, typer.Option(help="Remove existing fields missing from the config")] = False,
            rebuild_fields: Annotated[bool, typer.Option(help="Rebuild existing fields from the config")] = False):
    client = get_client()
    cuid = Cuid(length=18)

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
                  BarColumn(),
                  TaskProgressColumn(),
                  console=console) as progress:
        task = progress.add_task("Fetching database configuration...", total=None)

        # 1. Get database tree
        with handle_api_errors(f"Could not get tree for {target_database_id}"):
            target_tree = client.api.get_database_tree(target_database_id)

        # 2. Identify forms for levels
        # Level 1: 1.1 CDE
        cde_form = next((res for res in target_tree.resources if res.label.startswith("1.1")), None)
        # Level 2: 1.2 LFE
        lfe_form = next((res for res in target_tree.resources if res.label.startswith("1.2")), None)

        # Level 3: Entity forms from 0.1.1
        entity_config_res = next((res for res in target_tree.resources if res.label.startswith(ENTITY_CONFIG_FORM_011)),
                                 None)
        level3_forms: List[Dict[str, Any]] = []
        if entity_config_res:
            with handle_api_errors(f"Could not fetch records from {ENTITY_CONFIG_FORM_011}"):
                e_records = client.api.get_form(entity_config_res.id)
                for rec in e_records:
                    prefix = rec.get("SYSPREFIX")
                    refcode = rec.get("REFCODE")
                    if prefix:
                        f_res = next((res for res in target_tree.resources if res.label.startswith(prefix)), None)
                        if f_res:
                            level3_forms.append({"resource": f_res, "refcode": refcode})

        # Level 4: Data forms from 0.1.2
        data_config_res = next((res for res in target_tree.resources if res.label.startswith(DATA_CONFIG_FORM_012)),
                               None)
        level4_forms: List[Dict[str, Any]] = []
        if data_config_res:
            with handle_api_errors(f"Could not fetch records from {DATA_CONFIG_FORM_012}"):
                d_records = client.api.get_form(data_config_res.id)
                for rec in d_records:
                    sysname = rec.get("SYSNAME")
                    ccode = rec.get("CCODE")
                    eform_refcode = rec.get("EFORM.REFCODE") or rec.get("EFORM_REFCODE")
                    if sysname:
                        f_res = next((res for res in target_tree.resources if res.label == sysname), None)
                        if f_res:
                            level4_forms.append({"resource": f_res, "ccode": ccode, "eform_refcode": eform_refcode})

        # 3. Get 0.3.1 config records
        seg_config_res = next((res for res in target_tree.resources if res.label.startswith(SEG_CONFIG_FORM_PREFIX)),
                              None)
        if not seg_config_res:
            console.print(f"[red]Error: {SEG_CONFIG_FORM_PREFIX} not found.[/red]")
            raise typer.Exit(1)

        progress.update(task, description=f"Fetching records from {seg_config_res.label}...")
        with handle_api_errors(f"Could not fetch records from {seg_config_res.label}"):
            seg_records = get_records_with_multiref(client, seg_config_res.id)

        # Sort: SEGDIM.REFORDER then SEGLEVEL.REFLEVEL
        seg_records.sort(key=lambda r: (int(r.get("SEGDIM.REFORDER") or r.get("SEGDIM_REFORDER") or 0), 
                                         int(r.get("SEGLEVEL.REFLEVEL") or r.get("SEGLEVEL_REFLEVEL") or 0)))

        # 4. Group by SEGDIM
        grouped_seg_records: Dict[str, List[Dict[str, Any]]] = {}
        for r in seg_records:
            seg_dim_code = r.get("SEGDIM.REFCODE") or r.get("SEGDIM_REFCODE")
            if not seg_dim_code: continue
            if seg_dim_code not in grouped_seg_records:
                grouped_seg_records[seg_dim_code] = []
            grouped_seg_records[seg_dim_code].append(r)

        # 5. Process SEGDIM blocks
        progress.update(task, description="Processing segmentation...", total=len(grouped_seg_records))

        # We'll need a way to track all modified forms and update them once at the end or as we go.
        # Given the complexity, updating as we go is safer but might be slower.
        # We also need to cache schemas to avoid redundant GETs.
        schema_cache: Dict[str, FormSchema] = {}

        def get_cached_schema(form_id: str) -> FormSchema:
            if form_id not in schema_cache:
                schema_cache[form_id] = client.api.get_form_schema(form_id)
            return schema_cache[form_id]

        for seg_dim_code, records in grouped_seg_records.items():
            progress.update(task, description=f"Processing SEGDIM: {seg_dim_code}")

            # Find initial level
            min_level = min(int(r.get("SEGLEVEL.REFLEVEL") or r.get("SEGLEVEL_REFLEVEL") or 5) for r in records)
            if min_level > 4: continue

            # For each level from min_level to 4
            for current_level in range(min_level, 5):
                level_record = next((r for r in records if int(r.get("SEGLEVEL.REFLEVEL") or r.get("SEGLEVEL_REFLEVEL") or 0) == current_level), None)
                is_initial = (current_level == min_level)

                # Identify target forms
                targets: List[Tuple[FormSchema, Dict[str, Any], bool]] = []  # (schema, meta, is_explicit)

                if current_level == 1:
                    if cde_form:
                        targets.append((get_cached_schema(cde_form.id), {}, True))
                elif current_level == 2:
                    if lfe_form:
                        targets.append((get_cached_schema(lfe_form.id), {}, True))
                elif current_level == 3:
                    # Targets are level3_forms
                    # Filter if level_record is explicit
                    if level_record:
                        # explicit target logic
                        # those whose REFCODE is in EFORMS[x].REFCODE
                        eforms = level_record.get("EFORMS", [])
                        explicit_refcodes = {f.get("REFCODE") for f in eforms if f.get("REFCODE")}
                        for l3 in level3_forms:
                            is_explicit = not explicit_refcodes or l3["refcode"] in explicit_refcodes
                            if is_explicit or not is_initial:
                                targets.append((get_cached_schema(l3["resource"].id), l3, is_explicit))
                    elif not is_initial:
                        # inheritance only
                        for l3 in level3_forms:
                            targets.append((get_cached_schema(l3["resource"].id), l3, False))
                elif current_level == 4:
                    if level_record:
                        # explicit target logic
                        dforms = level_record.get("DFORMS", [])
                        explicit_ccodes = {f.get("CCODE") for f in dforms if f.get("CCODE")}
                        eforms = level_record.get("EFORMS", [])
                        explicit_refcodes = {f.get("REFCODE") for f in eforms if f.get("REFCODE")}

                        for l4 in level4_forms:
                            is_explicit = False
                            if explicit_ccodes:
                                if l4["ccode"] in explicit_ccodes: is_explicit = True
                            elif explicit_refcodes:
                                if l4["eform_refcode"] in explicit_refcodes: is_explicit = True
                            else:
                                is_explicit = True

                            if is_explicit or not is_initial:
                                targets.append((get_cached_schema(l4["resource"].id), l4, is_explicit))
                    elif not is_initial:
                        for l4 in level4_forms:
                            targets.append((get_cached_schema(l4["resource"].id), l4, False))

                # Process each target form
                for schema, meta, is_explicit in targets:
                    # Find insertion point
                    insertion_index = 0
                    if current_level in [1, 2]:
                        # After NAME
                        for i, el in enumerate(schema.elements):
                            if el.code == "NAME":
                                insertion_index = i + 1
                                break
                    elif current_level == 3:
                        # After 'Additional Details' section
                        for i, el in enumerate(schema.elements):
                            if el.type == FieldType.section and el.label == "Additional Details":
                                insertion_index = i + 1
                                break
                    elif current_level == 4:
                        # After ID fields, before DISAG_, before AMOUNT_/METRIC_
                        for i, el in enumerate(schema.elements):
                            if el.code in ["PROJ", "IND", "CSL", "CST"] or el.code.startswith("SEG_"):
                                insertion_index = i + 1
                            if (el.code.startswith("DISAG_") or is_metric_field(el.code)) and insertion_index == 0:
                                # This case handles if we haven't found a SEG_ yet
                                pass

                    # Locate existing seg field
                    existing_field = next((el for el in schema.elements if el.code == seg_dim_code), None)

                    # If rebuild_fields is false and field exists, we might still need to move it or skip.
                    # For simplicity, if it exists and we're not rebuilding, we just ensure it's in a 'seg' area.
                    if existing_field and not rebuild_fields:
                        continue

                    # Prep field(s)
                    new_fields: List[SchemaFieldDTO] = []

                    # Common variables from record
                    # We use the level_record if present, else we might need to inherit info from min_level record
                    ref_rec = level_record if level_record else records[0]
                    seg_dim_name = ref_rec.get("SEGDIM.NAME") or ref_rec.get("SEGDIM_NAME")
                    seg_dim_type = ref_rec.get("SEGDIM.TYPE") or ref_rec.get("SEGDIM_TYPE")
                    optmand = ref_rec.get("OPTMAND")
                    required = (optmand == "Mandatory")

                    # Relevance Helper
                    def get_relevance(lvl, rec, meta_info):
                        cdls = rec.get("CDLS", [])
                        lfls = rec.get("LFLS", [])
                        atypes = rec.get("ATYPES", [])
                        etypes = rec.get("ETYPES", [])

                        c1 = "|".join([c.get("REFCODE") for c in cdls if c.get("REFCODE")])
                        c2 = "|".join([l.get("REFCODE") for l in lfls if l.get("REFCODE")])
                        c3 = "|".join([a.get("REFCODE") for a in atypes if a.get("REFCODE")])

                        prefix = ""
                        if lvl == 4:
                            prefix = (meta_info.get("eform_refcode") or "") + "."

                        parts = []
                        if lvl == 1:
                            if c1: parts.append(f'REGEXMATCH(CDL.REFCODE, "^({c1})$")')
                        elif lvl == 2:
                            if c2: parts.append(f'REGEXMATCH(LFL.REFCODE, "^({c2})$")')
                        elif lvl >= 3:
                            sub_parts = []
                            et_codes = {e.get("REFCODE") for e in etypes if e.get("REFCODE")}
                            if "CDE" in et_codes or not et_codes:
                                r1 = f'REGEXMATCH({prefix}CDE.CDL.REFCODE, "^({c1})$")' if c1 else "TRUE"
                                sub_parts.append(f'({prefix}ETYPE.REFCODE == "CDE" && {r1})')
                            if "LFE" in et_codes or not et_codes:
                                r2 = f'REGEXMATCH({prefix}LFE.LFL.REFCODE, "^({c2})$")' if c2 else "TRUE"
                                sub_parts.append(f'({prefix}ETYPE.REFCODE == "LFE" && {r2})')

                            if sub_parts:
                                parts.append("(" + " || ".join(sub_parts) + ")")

                            if c3:
                                parts.append(f'REGEXMATCH({prefix}ATYPE.REFCODE, "^({c3})$")')

                        return " && ".join(parts) if parts else "TRUE"

                    # Inheritance Formula Helper
                    def get_inheritance_formula(lvl, meta_info):
                        code = seg_dim_code
                        if lvl == 2: return f"CDE.{code}"
                        if lvl == 3: return f'IF(ETYPE.REFCODE == "CDE", CDE.{code}, LFE.{code})'
                        if lvl == 4:
                            p = (meta_info.get("eform_refcode") or "")
                            return f"{p}.{code}"
                        return ""

                    # Validation Condition Helpers
                    def get_val_cond(rec, lvl, meta_info):
                        seg_type = rec.get("SEGDIM.TYPE") or rec.get("SEGDIM_TYPE")
                        seg_code = rec.get("SEGDIM.REFCODE") or rec.get("SEGDIM_REFCODE")
                        if seg_type == "Entity":
                            if seg_code == "SEG_GLOBCDE_MAP":
                                glob_refcode = rec.get("GLOBCDL.REFCODE") or rec.get("GLOBCDL_REFCODE")
                                prefix = ""
                                if lvl == 4: prefix = (meta_info.get("eform_refcode") or "") + "."
                                return f'SEG_GLOBCDE_MAP.GLOBCDL.REFCODE == "{glob_refcode}" && (ISBLANK({prefix}CDE.GLOBCDES_MAP) || !ISBLANK(SEARCH(CONCAT("|", SEG_GLOBCDE_MAP.CCODE, "|"), CONCAT("|", TEXTJOIN("|", TRUE, {prefix}CDE.GLOBCDES_MAP.REL_GLOBCDES_MAP), "|"))))'
                            if seg_code == "SEG_LFE":
                                lfl_refcode = rec.get("LFL.REFCODE") or rec.get("LFL_REFCODE")
                                prefix = ""
                                if lvl in [3, 4]: prefix = (meta_info.get("eform_refcode") or "") + "."
                                return f'SEG_LFE.LFL.REFCODE == "{lfl_refcode}" && ({prefix}ETYPE.REFCODE == "CDE" || !ISBLANK(SEARCH(CONCAT("|", SEG_LFE.CCODE, "|"), {prefix}LFE.HIGHER_LFES)))'
                        if seg_type == "Partner":
                            role = rec.get("SEGDIM.ORGROLE.REFCODE") or rec.get("SEGDIM_ORGROLE_REFCODE")
                            return f'!ISBLANK(SEARCH("|{role}|", CONCAT("|", TEXTJOIN("|", TRUE, {seg_code}.ORGROLES.REFCODE), "|"))) && !ISBLANK(SEARCH(CONCAT("|", {seg_code}.ORG.NAME, "|"), CONCAT("|", TEXTJOIN("|", TRUE, PROJECT.{role}ORGS.ORG.NAME), "|")))'
                        return ""

                    if is_initial or is_explicit:
                        # Create reference field
                        field_code = seg_dim_code if is_initial else f"{seg_dim_code}_MAN"
                        label: str = ""
                        form_id: str = ""

                        if seg_dim_type == "Reference":
                            label = str(ref_rec.get("RFORM.NAME") or ref_rec.get("RFORM_NAME") or "")
                            sysname = ref_rec.get("SYSNAME")
                            ref_res = next((r for r in target_tree.resources if r.label == sysname), None)
                            form_id = ref_res.id if ref_res else ""
                        elif seg_dim_type == "Entity":
                            if seg_dim_code == "SEG_GLOBCDE_MAP":
                                label = str(ref_rec.get("GLOBCDL.NAME") or ref_rec.get("GLOBCDL_NAME") or "")
                                prefix = ref_rec.get("GLOBCDL.EFORM.SYSPREFIX") or ref_rec.get("GLOBCDL_EFORM_SYSPREFIX")
                                # This needs to be found in target_tree or it might be in GRM.
                                # Spec says HARDCODED G2.4C in GRM. For now look in target_tree.
                                ref_res = next((r for r in target_tree.resources if r.label.startswith("G2.4C")), None)
                                form_id = ref_res.id if ref_res else ""
                            elif seg_dim_code == "SEG_LFE":
                                label = str(ref_rec.get("LFL.NAME") or ref_rec.get("LFL_NAME") or "")
                                form_id = lfe_form.id if lfe_form else ""
                        elif seg_dim_type == "Partner":
                            label = str(seg_dim_name or "")
                            # Spec says 2.1 HARDCODED
                            ref_res = next((r for r in target_tree.resources if r.label.startswith("2.1")), None)
                            form_id = ref_res.id if ref_res else ""

                        rel_cond = ""
                        if is_initial:
                            rel_cond = get_relevance(current_level, ref_rec, meta)
                        else:
                            # ISBLANK(PARENT.{code}) && {current_level_relevance}
                            parent_code = seg_dim_code
                            parent_ref = ""
                            if current_level == 2:
                                parent_ref = "CDE."
                            elif current_level == 3:
                                parent_ref = 'IF(ETYPE.REFCODE == "CDE", CDE., LFE.)'  # This is tricky for formula
                                # Actually the spec says: (ETYPE.REFCODE == "CDE" && ISBLANK(CDE.{1}) || ETYPE.REFCODE == "LFE" && ISBLANK(LFE.{1})) && {3}
                            elif current_level == 4:
                                parent_ref = (str(meta.get("eform_refcode") or "")) + "."

                            if current_level == 3:
                                rel_cond = f'(ETYPE.REFCODE == "CDE" && ISBLANK(CDE.{parent_code}) || ETYPE.REFCODE == "LFE" && ISBLANK(LFE.{parent_code})) && {get_relevance(current_level, ref_rec, meta)}'
                            else:
                                rel_cond = f'ISBLANK({parent_ref}{parent_code}) && {get_relevance(current_level, ref_rec, meta)}'

                        val_cond = get_val_cond(ref_rec, current_level, meta)

                        ref_field = SchemaFieldDTO(
                            id=cuid.generate(),
                            code=field_code,
                            label=label or str(seg_dim_name or ""),
                            required=True if (seg_dim_type == "Partner") else (required if is_initial else True),
                            relevanceCondition=rel_cond,
                            validationCondition=val_cond,
                            type=FieldType.reference,
                            typeParameters=FieldTypeParametersUpdateDTO(
                                cardinality="single",
                                range=[{"formId": form_id}],
                                lookupConfigs=[
                                    TypeParameterLookupConfig(id=cuid.generate(), formula="REFLABEL",
                                                              lookupLabel="Reference Label")]
                            )
                        )
                        if seg_dim_type == "Partner":
                            ref_field.key = True

                        new_fields.append(ref_field)

                        if not is_initial:
                            # Add companion inheritance field
                            inh_field = SchemaFieldDTO(
                                id=cuid.generate(),
                                code=seg_dim_code,
                                label=label or str(seg_dim_name or ""),
                                required=False,
                                type=FieldType.calculated,
                                dataEntryVisible=False,
                                tableVisible=False,
                                typeParameters=FieldTypeParametersUpdateDTO(
                                    formula=f"COALESCE({ref_field.id}, {get_inheritance_formula(current_level, meta)})"
                                )
                            )
                            new_fields.append(inh_field)
                    else:
                        # Standalone inheritance field
                        label = str(seg_dim_name or "")  # Might need better label
                        inh_field = SchemaFieldDTO(
                            id=cuid.generate(),
                            code=seg_dim_code,
                            label=label,
                            required=False,
                            type=FieldType.calculated,
                            dataEntryVisible=False,
                            tableVisible=False,
                            typeParameters=FieldTypeParametersUpdateDTO(
                                formula=get_inheritance_formula(current_level, meta)
                            )
                        )
                        new_fields.append(inh_field)



                    # Update schema.elements
                    # Remove existing if any
                    existing_indices = [i for i, el in enumerate(schema.elements) if
                                        el.code == seg_dim_code or el.code == f"{seg_dim_code}_MAN"]
                    for idx in sorted(existing_indices, reverse=True):
                        schema.elements.pop(idx)
                        if idx < insertion_index:
                            insertion_index -= 1

                    # Insert new fields
                    for f in reversed(new_fields):
                        schema.elements.insert(insertion_index, f)

            progress.advance(task)

        # 6. Final Schema Updates
        progress.update(task, description="Saving form schemas...")
        for form_id, schema in schema_cache.items():
            with handle_api_errors(f"Could not update schema for form {form_id}"):
                client.api.update_form_schema(schema)

    console.print("[bold green]Success:[/bold green] Segmentation fields adjusted.")


if __name__ == "__main__":
    app()
