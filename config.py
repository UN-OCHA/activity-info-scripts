from itertools import groupby
from typing import Annotated, Optional, List, Dict

import typer
from cuid2 import Cuid
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from api.models import SchemaFieldDTO, FieldTypeParametersUpdateDTO, FieldType, TypeParameterLookupConfig, \
    FormSchema
from common import filter_data_forms, get_records_with_multiref
from utils import get_client, console, handle_api_errors

# Initialize Typer sub-application for field-level configuration adjustments
app = typer.Typer(no_args_is_help=True)

# Standard prefixes for configuration forms that drive the logic in this script
METRIC_CONFIG_FORM_PREFIX = "0.3.3"  # Configuration for Amount/Metric fields
DISAGG_CONFIG_FORM_PREFIX = "0.3.2"  # Configuration for Disaggregation fields
SEG_CONFIG_FORM_PREFIX = "0.3.1"  # Configuration for Segmentation fields
DATA_CONFIG_FORM_012 = "0.1.2"  # Base Data Form definitions
ENTITY_CONFIG_FORM_011 = "0.1.1"  # Entity Form definitions


def strip_metric_prefix(code: str) -> str:
    """Removes 'AMOUNT_' or 'METRIC_' prefixes from field codes."""
    if code.startswith("AMOUNT_"):
        return code[len("AMOUNT_"):]
    if code.startswith("METRIC_"):
        return code[len("METRIC_"):]
    return code


def is_metric_field(code: str) -> bool:
    """Checks if a field code corresponds to an Amount or Metric field."""
    return code.startswith("AMOUNT_") or code.startswith("METRIC_")


def is_disag_field(code: str) -> bool:
    """Checks if a field code corresponds to a Disaggregation field."""
    return code.startswith("DISAG_")


def get_metric_base_code(code: str) -> str:
    """
    Extracts the base base code from a metric field by stripping prefixes and 
    standard suffixes (_MAN, _ECALC, _ICALC).
    """
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
    """
    Synchronize 'Amount' fields (Metrics) in data forms based on the 0.3.3 configuration.
    
    This command creates a standardized set of 4 fields for each metric:
    1. Manual input (_MAN)
    2. External calculation (_ECALC)
    3. Internal calculation (_ICALC)
    4. A final Coalesced field (Final)
    """
    client = get_client()
    cuid = Cuid(length=18)

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
                  BarColumn(),
                  TaskProgressColumn(),
                  console=console) as progress:

        # --- 1. Initialization ---
        task = progress.add_task("Fetching database configuration...", total=None)

        # --- 2. Retrieve State ---
        with handle_api_errors(f"Could not get tree for {target_database_id}"):
            target_tree = client.api.get_database_tree(target_database_id)

        # --- 3. Identify Target Data Forms ---
        # First, find forms in folders starting with 3, 4, 5, 6
        folder_forms = filter_data_forms(target_tree, root_folder_id or target_database_id)

        # Second, find forms explicitly mentioned in 0.1.2 config form
        config_form_012 = next((res for res in target_tree.resources if res.label.startswith("0.1.2")), None)
        config_form_names = set()
        if config_form_012:
            with handle_api_errors("Could not obtain 0.1.2 config form records"):
                config_records = client.api.get_form(config_form_012.id)
                for rec in config_records:
                    name = rec.get("SYSNAME") or rec.get("SYS_NAME")
                    if name:
                        config_form_names.add(name)

        # Merge these lists to get the exhaustive set of data forms to process
        data_forms = [res for res in target_tree.resources if res.label in config_form_names]
        existing_ids = {res.id for res in data_forms}
        for res in folder_forms:
            if res.id not in existing_ids:
                data_forms.append(res)

        data_forms_by_name = {res.label: res for res in data_forms}

        # --- 4. Read Metric Configuration (0.3.3) ---
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

        # Sort and group records by target form name
        records.sort(key=lambda r: (r.get("DFORM.SYSNAME") or "", r.get("REFORDER") or ""))
        form_records_by_sysname = {
            sysname: list(items)
            for sysname, items in
            groupby(records, key=lambda r: r.get("DFORM.SYSNAME") or r.get("DFORM_SYSNAME") or r.get("SYSNAME"))
            if sysname
        }

        # --- 5. Process each target form ---
        progress.update(task, description="Adjusting metric fields...", total=len(data_forms_by_name))

        for sysname, target_form_res in data_forms_by_name.items():
            progress.update(task, description=f"Processing form: {sysname}")

            form_records = form_records_by_sysname.get(sysname, [])

            # Skip if no config and we aren't cleaning up missing fields
            if not form_records and not remove_fields:
                progress.advance(task)
                continue

            with handle_api_errors(f"Could not process schema for {sysname}"):
                schema = client.api.get_form_schema(target_form_res.id)

                # --- Find Insertion Point ---
                # Metrics should appear after ID and Segmentation fields but before other custom fields
                insertion_index = 0
                for i, element in enumerate(schema.elements):
                    code = element.code
                    if code in ["PROJ", "IND", "CSL", "CST"] or code.startswith("SEG_") or code.startswith("DISAG_"):
                        insertion_index = i + 1

                # Partition elements for easier injection
                non_metric_before = schema.elements[:insertion_index]
                rest_elements = schema.elements[insertion_index:]
                other_elements = [e for e in rest_elements if not is_metric_field(e.code)]
                existing_metric_elements = [e for e in rest_elements if is_metric_field(e.code)]

                # Group existing metrics by their base code
                metric_schemas: Dict[str, List[SchemaFieldDTO]] = {}
                for e in existing_metric_elements:
                    base_code = get_metric_base_code(e.code)
                    if base_code not in metric_schemas:
                        metric_schemas[base_code] = []
                    metric_schemas[base_code].append(e)

                final_metric_base_codes = []
                processed_base_codes = set()

                # --- Apply Metric Transformations from Config ---
                for record in form_records:
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

                    needs_rebuild = rebuild_fields
                    existing_schema_elements = metric_schemas.get(base_code)

                    if existing_schema_elements and not needs_rebuild:
                        continue

                    # Generate new set of IDs or preserve existing ones for stability
                    ids = [cuid.generate() for _ in range(4)]
                    if existing_schema_elements:
                        for i, suffix in enumerate(["_MAN", "_ECALC", "_ICALC", ""]):
                            full_code = f"AMOUNT_{base_code}{suffix}" if suffix else f"AMOUNT_{base_code}"
                            alt_code = f"METRIC_{base_code}{suffix}" if suffix else f"METRIC_{base_code}"
                            found = next(
                                (e for e in existing_schema_elements if e.code == full_code or e.code == alt_code),
                                None)
                            if found:
                                ids[i] = found.id

                    # Define visibility conditions based on indicator/process configuration
                    relevance_man = f'!ISBLANK(SEARCH("|{ccode}|", CONCAT("|", TEXTJOIN("|", TRUE, {eform_refcode}.MM.METRICS_MAN.CCODE), "|")))'
                    relevance_others = f'!ISBLANK(SEARCH("|{ccode}|", CONCAT("|", TEXTJOIN("|", TRUE, {eform_refcode}.MM.METRICS_MAN.CCODE), "|"))) || !ISBLANK(SEARCH("|{ccode}|", CONCAT("|", TEXTJOIN("|", TRUE, {eform_refcode}.MM.METRICS_CALC.CCODE), "|")))'

                    new_elements = []

                    # 1. Manual Entry Field
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

                    # 2. External Calculation (imported/pushed from external tools)
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

                    # 3. Internal Calculation (calculated via formulas within ActivityInfo)
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

                    # 4. Final Display Field (Coalesce logic to pick the best available value)
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

                # --- Handle Removals ---
                remaining_base_codes = [bc for bc in metric_schemas.keys() if bc not in processed_base_codes]
                if remove_fields:
                    for bc in remaining_base_codes:
                        if bc in metric_schemas:
                            del metric_schemas[bc]
                else:
                    for bc in remaining_base_codes:
                        final_metric_base_codes.append(bc)

                # Reassemble the final schema elements list in the correct order
                final_metric_elements = []
                for bc in final_metric_base_codes:
                    if bc in metric_schemas:
                        final_metric_elements.extend(metric_schemas[bc])

                schema.elements = non_metric_before + final_metric_elements + other_elements
                client.api.update_form_schema(schema)

            progress.advance(task)

    console.print("[bold green]Success:[/bold green] Metric fields adjusted.")


@app.command(help="Adjust disaggregation fields in data forms", no_args_is_help=True)
def disagg(target_database_id: Annotated[str, typer.Argument(help="The ID of the target database")],
           root_folder_id: Annotated[
               Optional[str], typer.Argument(help="The root folder ID of the data folders (optional)")] = None,
           remove_fields: Annotated[bool, typer.Option(help="Remove existing fields missing from the config")] = False,
           rebuild_fields: Annotated[bool, typer.Option(help="Rebuild existing fields from the config")] = False):
    """
    Synchronize 'Disaggregation' fields (References) based on the 0.3.2 configuration.
    
    These fields act as keys in the data form and point to specific reference forms 
    (e.g., Age/Gender groups, Locations).
    """
    client = get_client()
    cuid = Cuid(length=18)

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
                  BarColumn(),
                  TaskProgressColumn(),
                  console=console) as progress:

        # --- Initialization & State ---
        task = progress.add_task("Fetching database configuration...", total=None)
        with handle_api_errors(f"Could not get tree for {target_database_id}"):
            target_tree = client.api.get_database_tree(target_database_id)

        # Retrieve exhaustive list of target forms
        folder_forms = filter_data_forms(target_tree, root_folder_id or target_database_id)
        config_form_012 = next((res for res in target_tree.resources if res.label.startswith("0.1.2")), None)
        config_form_names = set()
        if config_form_012:
            with handle_api_errors("Could not obtain 0.1.2 config form records"):
                config_records = client.api.get_form(config_form_012.id)
                for rec in config_records:
                    name = rec.get("SYSNAME") or rec.get("SYS_NAME")
                    if name: config_form_names.add(name)

        data_forms = [res for res in target_tree.resources if res.label in config_form_names]
        existing_ids = {res.id for res in data_forms}
        for res in folder_forms:
            if res.id not in existing_ids: data_forms.append(res)

        data_forms_by_name = {res.label: res for res in data_forms}
        all_forms_by_name = {res.label: res for res in target_tree.resources if res.type == "FORM"}

        # --- Read Disaggregation Configuration (0.3.2) ---
        disagg_config_form = next(
            (res for res in target_tree.resources if res.label.startswith(DISAGG_CONFIG_FORM_PREFIX)), None)
        if not disagg_config_form:
            progress.stop()
            console.print(f"[bold red]Error:[/bold red] Configuration form {DISAGG_CONFIG_FORM_PREFIX} not found.")
            raise typer.Exit(code=1)

        progress.update(task, description=f"Fetching records from {disagg_config_form.label}...")
        with handle_api_errors(f"Could not get records for {disagg_config_form.id}"):
            records = client.api.get_form(disagg_config_form.id)

        records.sort(key=lambda r: (r.get("DFORM.SYSNAME") or "", r.get("REFORDER") or ""))
        form_records_by_sysname = {
            sysname: list(items)
            for sysname, items in
            groupby(records, key=lambda r: r.get("DFORM.SYSNAME") or r.get("DFORM_SYSNAME") or r.get("SYSNAME"))
            if sysname
        }

        # --- Process each form ---
        progress.update(task, description="Adjusting disaggregation fields...", total=len(data_forms_by_name))

        for sysname, target_form_res in data_forms_by_name.items():
            progress.update(task, description=f"Processing form: {sysname}")

            form_records = form_records_by_sysname.get(sysname, [])
            if not form_records and not remove_fields:
                progress.advance(task)
                continue

            with handle_api_errors(f"Could not process schema for {sysname}"):
                schema = client.api.get_form_schema(target_form_res.id)

                # Find injection point (after segments, before metrics)
                insertion_index = 0
                for i, element in enumerate(schema.elements):
                    code = element.code
                    if code in ["PROJ", "IND", "CSL", "CST"] or code.startswith("SEG_"):
                        insertion_index = i + 1

                elements_before = schema.elements[:insertion_index]
                rest_elements = schema.elements[insertion_index:]
                disag_elements = [e for e in rest_elements if is_disag_field(e.code)]
                other_elements = [e for e in rest_elements if not is_disag_field(e.code)]

                existing_disags_by_code = {e.code: e for e in disag_elements}
                final_disag_codes = []
                processed_codes = set()

                for record in form_records:
                    ref_code = record.get("REFCODE")
                    if not ref_code: continue
                    if ref_code in processed_codes: continue

                    final_disag_codes.append(ref_code)
                    processed_codes.add(ref_code)

                    name = record.get("NAME")
                    ccode = record.get("CCODE")
                    eform_refcode = record.get("DFORM.EFORM.REFCODE") or record.get("DFORM_EFORM_REFCODE")
                    rform_sysname = record.get("RFORM.SYSNAME") or record.get("RFORM_SYSNAME")

                    existing_element = existing_disags_by_code.get(ref_code)
                    if existing_element and not rebuild_fields: continue

                    # Resolve the reference form ID
                    ref_form = all_forms_by_name.get(rform_sysname)
                    if not ref_form:
                        console.print(f"[yellow]Warning: Reference form {rform_sysname} not found. Skipping.[/yellow]")
                        continue

                    # Construct dynamic relevance condition
                    relevance = f'!ISBLANK(SEARCH("|{ref_code}|", CONCAT("|", TEXTJOIN("|", TRUE, {eform_refcode}.DM.RFORMS_OPT.REFCODE), "|"))) || !ISBLANK(SEARCH("|{ccode}|", CONCAT("|", TEXTJOIN("|", TRUE, {eform_refcode}.DM.DISAGCONFIGS_OPT.CCODE), "|")))'

                    new_element = SchemaFieldDTO(
                        id=existing_element.id if existing_element else cuid.generate(),
                        code=ref_code,
                        label=name,
                        required=True,  # References in disaggregations are almost always keys/required
                        type=FieldType.reference,
                        key=True,
                        relevanceCondition=relevance,
                        typeParameters=FieldTypeParametersUpdateDTO(
                            cardinality="single",
                            range=[{"formId": ref_form.id}],
                            lookupConfigs=[TypeParameterLookupConfig(id=cuid.generate(), formula="REFLABEL",
                                                                     lookupLabel="Reference Label")]
                        )
                    )
                    existing_disags_by_code[ref_code] = new_element

                # --- Removal & Data Integrity ---
                # Removing fields with existing data is restricted by the API.
                # We optionally delete records containing data in fields about to be removed.
                remaining_codes = [c for c in existing_disags_by_code.keys() if c not in processed_codes]
                if remove_fields and remaining_codes:
                    with handle_api_errors(f"Checking for data in fields being removed from {sysname}"):
                        records_to_check = client.api.get_form(target_form_res.id)

                    records_to_delete = []
                    for rec in records_to_check:
                        should_delete = False
                        for c in remaining_codes:
                            existing_field = existing_disags_by_code.get(c)
                            f_id = existing_field.id if existing_field else None
                            if rec.get(c) or rec.get(f"{c}.@id") or (
                                    f_id and (rec.get(f_id) or rec.get(f"{f_id}.@id"))):
                                should_delete = True
                                break
                        if should_delete: records_to_delete.append(rec["@id"])

                    if records_to_delete:
                        from api.models import RecordUpdateDTO
                        client.api.update_form_records(
                            [RecordUpdateDTO(formId=target_form_res.id, recordId=rid, deleted=True, fields={}) for rid
                             in records_to_delete])

                    for c in remaining_codes:
                        if c in existing_disags_by_code: del existing_disags_by_code[c]
                else:
                    for c in remaining_codes: final_disag_codes.append(c)

                # Finalize elements list
                final_ordered_disags = [existing_disags_by_code[c] for c in final_disag_codes if
                                        c in existing_disags_by_code]
                schema.elements = elements_before + final_ordered_disags + other_elements
                client.api.update_form_schema(schema)

            progress.advance(task)

    console.print("[bold green]Success:[/bold green] Disaggregation fields adjusted.")


@app.command(help="Adjust segmentation fields in CDE, LFE, IND/CST/CSL and data forms", no_args_is_help=True)
def segment(target_database_id: Annotated[str, typer.Argument(help="The ID of the target database")],
            remove_fields: Annotated[bool, typer.Option(help="Remove existing fields missing from the config")] = False,
            rebuild_fields: Annotated[bool, typer.Option(help="Rebuild existing fields from the config")] = False):
    """
    Synchronize 'Segmentation' fields (Hierarchical filters) based on the 0.3.1 configuration.
    
    This command implements complex inheritance logic:
    Level 1 (Coordination Entity) -> Level 2 (Logframe Entity) -> Level 3 (Activity Entity) -> Level 4 (Data Form).
    Fields are either explicit manual entries or inherited calculations from the parent level.
    """
    client = get_client()
    cuid = Cuid(length=18)

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
                  BarColumn(),
                  TaskProgressColumn(),
                  console=console) as progress:
        task = progress.add_task("Fetching database configuration...", total=None)

        # --- 1. Map Hierarchy Levels ---
        with handle_api_errors(f"Could not get tree for {target_database_id}"):
            target_tree = client.api.get_database_tree(target_database_id)

        cde_form = next((res for res in target_tree.resources if res.label.startswith("1.1")), None)
        lfe_form = next((res for res in target_tree.resources if res.label.startswith("1.2")), None)

        # Identify Level 3 forms (Activities/Entities)
        entity_config_res = next((res for res in target_tree.resources if res.label.startswith(ENTITY_CONFIG_FORM_011)),
                                 None)
        level3_forms = []
        if entity_config_res:
            with handle_api_errors(f"Could not fetch {ENTITY_CONFIG_FORM_011}"):
                for rec in client.api.get_form(entity_config_res.id):
                    prefix = rec.get("SYSPREFIX")
                    if prefix:
                        f_res = next((res for res in target_tree.resources if res.label.startswith(prefix)), None)
                        if f_res: level3_forms.append({"resource": f_res, "refcode": rec.get("REFCODE")})

        # Identify Level 4 forms (Data entry forms)
        data_config_res = next((res for res in target_tree.resources if res.label.startswith(DATA_CONFIG_FORM_012)),
                               None)
        level4_forms = []
        if data_config_res:
            with handle_api_errors(f"Could not fetch {DATA_CONFIG_FORM_012}"):
                for rec in client.api.get_form(data_config_res.id):
                    sysname = rec.get("SYSNAME")
                    if sysname:
                        f_res = next((res for res in target_tree.resources if res.label == sysname), None)
                        if f_res: level4_forms.append(
                            {"resource": f_res, "ccode": rec.get("CCODE"), "eform_refcode": rec.get("EFORM.REFCODE")})

        # --- 2. Read Segmentation Config (0.3.1) ---
        seg_config_res = next((res for res in target_tree.resources if res.label.startswith(SEG_CONFIG_FORM_PREFIX)),
                              None)
        if not seg_config_res:
            console.print(f"[red]Error: {SEG_CONFIG_FORM_PREFIX} not found.[/red]")
            raise typer.Exit(1)

        with handle_api_errors(f"Could not fetch {seg_config_res.label}"):
            seg_records = get_records_with_multiref(client, seg_config_res.id)

        seg_records.sort(key=lambda r: (int(r.get("SEGDIM.REFORDER") or 0), int(r.get("SEGLEVEL.REFLEVEL") or 0)))
        grouped_seg_records = {}
        for r in seg_records:
            seg_dim_code = r.get("SEGDIM.REFCODE") or r.get("SEGDIM_REFCODE")
            if seg_dim_code: grouped_seg_records.setdefault(seg_dim_code, []).append(r)

        # --- 3. Process Segmentation logic across all levels ---
        progress.update(task, description="Processing segmentation...", total=len(grouped_seg_records))
        schema_cache: Dict[str, FormSchema] = {}

        def get_cached_schema(f_id: str) -> FormSchema:
            if f_id not in schema_cache: schema_cache[f_id] = client.api.get_form_schema(f_id)
            return schema_cache[f_id]

        for seg_dim_code, records in grouped_seg_records.items():
            progress.update(task, description=f"Processing SEGDIM: {seg_dim_code}")

            # Robust level extraction
            def get_level(r):
                val = r.get("SEGLEVEL.REFLEVEL") or r.get("SEGLEVEL_REFLEVEL")
                return int(val) if val is not None else 5

            min_level = min(get_level(r) for r in records)
            if min_level > 4: continue

            # Apply logic level-by-level to ensure inheritance formulas resolve correctly
            for current_level in range(min_level, 5):
                level_record = next((r for r in records if get_level(r) == current_level), None)
                is_initial = (current_level == min_level)

                targets = []
                if current_level == 1 and cde_form:
                    targets.append((get_cached_schema(cde_form.id), {}, True))
                elif current_level == 2 and lfe_form:
                    targets.append((get_cached_schema(lfe_form.id), {}, True))
                elif current_level == 3:
                    if level_record:
                        explicit_refcodes = {f.get("REFCODE") for f in level_record.get("EFORMS", []) if
                                             f.get("REFCODE")}
                        for l3 in level3_forms:
                            is_explicit = not explicit_refcodes or l3["refcode"] in explicit_refcodes
                            if is_explicit or not is_initial: targets.append(
                                (get_cached_schema(l3["resource"].id), l3, is_explicit))
                    elif not is_initial:
                        for l3 in level3_forms: targets.append((get_cached_schema(l3["resource"].id), l3, False))
                elif current_level == 4:
                    if level_record:
                        explicit_ccodes = {f.get("CCODE") for f in level_record.get("DFORMS", []) if f.get("CCODE")}
                        explicit_refcodes = {f.get("REFCODE") for f in level_record.get("EFORMS", []) if
                                             f.get("REFCODE")}
                        for l4 in level4_forms:
                            is_explicit = False
                            if explicit_ccodes:
                                if l4["ccode"] in explicit_ccodes: is_explicit = True
                            elif explicit_refcodes:
                                if l4["eform_refcode"] in explicit_refcodes: is_explicit = True
                            else:
                                is_explicit = True

                            if is_explicit or not is_initial: targets.append(
                                (get_cached_schema(l4["resource"].id), l4, is_explicit))
                    elif not is_initial:
                        for l4 in level4_forms: targets.append((get_cached_schema(l4["resource"].id), l4, False))

                # Inject Fields into Schema
                for schema, meta, is_explicit in targets:
                    # RE-CALCULATE insertion_index for every target to handle shared schemas
                    insertion_index = 0
                    if current_level in [1, 2]:
                        for i, el in enumerate(schema.elements):
                            if el.code == "NAME": insertion_index = i + 1; break
                    elif current_level == 3:
                        for i, el in enumerate(schema.elements):
                            if (el.type == FieldType.section and el.label == "Additional Details") or el.code == "ADD_DETAILS": 
                                insertion_index = i + 1; break
                    elif current_level == 4:
                        for i, el in enumerate(schema.elements):
                            if el.code in ["PROJ", "IND", "CSL", "CST"] or el.code.startswith("SEG_"): 
                                insertion_index = i + 1

                    existing_field = next((el for el in schema.elements if el.code == seg_dim_code), None)
                    if existing_field and not rebuild_fields: continue

                    new_fields = []
                    ref_rec = level_record if level_record else records[0]
                    seg_dim_name = ref_rec.get("SEGDIM.NAME") or ref_rec.get("SEGDIM_NAME")
                    seg_dim_type = ref_rec.get("SEGDIM.TYPE") or ref_rec.get("SEGDIM_TYPE")
                    optmand = ref_rec.get("OPTMAND")
                    required = (optmand == "Mandatory")

                    # --- Local Relevance Helper ---
                    def get_relevance(lvl, rec, meta_info):
                        def get_list(key1, key2):
                            items = rec.get(key1) or rec.get(key2) or []
                            return "|".join([str(i.get("REFCODE")) for i in items if i.get("REFCODE")])

                        cdls = get_list("CDLS", "CDLS")
                        lfls = get_list("LFLS", "LFLS")
                        atypes = get_list("ATYPES", "ATYPES")
                        
                        # Fix prefix logic: only add dot if prefix is NOT empty
                        eform_ref = meta_info.get("eform_refcode") or ""
                        prefix = f"{eform_ref}." if eform_ref else ""
                        
                        parts = []
                        if lvl == 1 and cdls:
                            parts.append(f'REGEXMATCH(CDL.REFCODE, "^({cdls})$")')
                        elif lvl == 2 and lfls:
                            parts.append(f'REGEXMATCH(LFL.REFCODE, "^({lfls})$")')
                        elif lvl >= 3:
                            et_items = rec.get("ETYPES") or []
                            et_codes = {e.get("REFCODE") for e in et_items if e.get("REFCODE")}
                            sub = []
                            if "CDE" in et_codes or not et_codes: sub.append(
                                f'({prefix}ETYPE.REFCODE == "CDE" && {f"REGEXMATCH({prefix}CDE.CDL.REFCODE, ^({cdls})$)" if cdls else "TRUE"})')
                            if "LFE" in et_codes or not et_codes: sub.append(
                                f'({prefix}ETYPE.REFCODE == "LFE" && {f"REGEXMATCH({prefix}LFE.LFL.REFCODE, ^({lfls})$)" if lfls else "TRUE"})')
                            if sub: parts.append("(" + " || ".join(sub) + ")")
                            if atypes: parts.append(f'REGEXMATCH({prefix}ATYPE.REFCODE, "^({atypes})$")')
                        return " && ".join(parts) if parts else "TRUE"

                    if is_initial or is_explicit:
                        # Create interactive Manual field if this is where the segmentation starts or is explicitly overridden
                        field_code = seg_dim_code if is_initial else f"{seg_dim_code}_MAN"
                        f_id = ""
                        if seg_dim_type == "Reference":
                            sysname = ref_rec.get("SYSNAME") or ref_rec.get("SYS_NAME")
                            f_id = next((r.id for r in target_tree.resources if r.label == sysname), "")
                        elif seg_dim_type == "Entity":
                            f_id = lfe_form.id if seg_dim_code == "SEG_LFE" else next(
                                (r.id for r in target_tree.resources if r.label.startswith("G2.4C")), "")
                        elif seg_dim_type == "Partner":
                            f_id = next((r.id for r in target_tree.resources if r.label.startswith("2.1")), "")

                        eform_ref = meta.get("eform_refcode") or ""
                        parent_ref = ""
                        if current_level == 2:
                            parent_ref = "CDE."
                        elif current_level == 3:
                            # Level 3 is special as it can have multiple parents
                            parent_ref = 'IF(ETYPE.REFCODE == "CDE", CDE., LFE.)' 
                        elif current_level == 4:
                            parent_ref = f"{eform_ref}." if eform_ref else ""

                        if is_initial:
                            rel = get_relevance(current_level, ref_rec, meta)
                        else:
                            if current_level == 3:
                                rel = f'(ETYPE.REFCODE == "CDE" && ISBLANK(CDE.{seg_dim_code}) || ETYPE.REFCODE == "LFE" && ISBLANK(LFE.{seg_dim_code})) && {get_relevance(current_level, ref_rec, meta)}'
                            else:
                                rel = f'ISBLANK({parent_ref}{seg_dim_code}) && {get_relevance(current_level, ref_rec, meta)}'

                        ref_field = SchemaFieldDTO(id=cuid.generate(), code=field_code, label=str(seg_dim_name or ""),
                                                   required=True if seg_dim_type == "Partner" else (
                                                       required if is_initial else True), relevanceCondition=rel,
                                                   type=FieldType.reference,
                                                   typeParameters=FieldTypeParametersUpdateDTO(cardinality="single",
                                                                                               range=[{"formId": f_id}],
                                                                                               lookupConfigs=[
                                                                                                   TypeParameterLookupConfig(
                                                                                                       id=cuid.generate(),
                                                                                                       formula="REFLABEL",
                                                                                                       lookupLabel="Reference Label")]))
                        if seg_dim_type == "Partner": ref_field.key = True
                        new_fields.append(ref_field)

                        if not is_initial:
                            # Add calculation field to coalesce manual input with inherited value from parent
                            eform_ref = meta.get("eform_refcode") or ""
                            inh_form = f"CDE.{seg_dim_code}" if current_level == 2 else (
                                f'IF(ETYPE.REFCODE == "CDE", CDE.{seg_dim_code}, LFE.{seg_dim_code})' if current_level == 3 else f'{eform_ref + "." if eform_ref else ""}{seg_dim_code}')
                            new_fields.append(
                                SchemaFieldDTO(id=cuid.generate(), code=seg_dim_code, label=str(seg_dim_name or ""),
                                               required=False, type=FieldType.calculated, dataEntryVisible=False,
                                               tableVisible=False, typeParameters=FieldTypeParametersUpdateDTO(
                                        formula=f"COALESCE({ref_field.id}, {inh_form})")))
                    else:
                        # Pure inheritance: this level just mirrors the value from the parent level
                        eform_ref = meta.get("eform_refcode") or ""
                        inh_form = f"CDE.{seg_dim_code}" if current_level == 2 else (
                            f'IF(ETYPE.REFCODE == "CDE", CDE.{seg_dim_code}, LFE.{seg_dim_code})' if current_level == 3 else f'{eform_ref + "." if eform_ref else ""}{seg_dim_code}')
                        new_fields.append(
                            SchemaFieldDTO(id=cuid.generate(), code=seg_dim_code, label=str(seg_dim_name or ""),
                                           required=False, type=FieldType.calculated, dataEntryVisible=False,
                                           tableVisible=False,
                                           typeParameters=FieldTypeParametersUpdateDTO(formula=inh_form)))

                    # Update the schema elements list
                    existing_indices = [i for i, el in enumerate(schema.elements) if
                                        el.code in [seg_dim_code, f"{seg_dim_code}_MAN"]]
                    for idx in sorted(existing_indices, reverse=True):
                        schema.elements.pop(idx)
                        if idx < insertion_index: insertion_index -= 1
                    for f in reversed(new_fields): 
                        schema.elements.insert(insertion_index, f)
                        insertion_index += 1 # Maintain order for next SEGDIM in SAME target

            progress.advance(task)

        # Final Batch Commit of all modified schemas
        progress.update(task, description="Saving form schemas...")
        for form_id, schema in schema_cache.items():
            with handle_api_errors(f"Updating {form_id}"): client.api.update_form_schema(schema)

    console.print("[bold green]Success:[/bold green] Segmentation fields adjusted.")


if __name__ == "__main__":
    app()
