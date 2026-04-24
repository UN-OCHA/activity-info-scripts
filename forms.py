import json
import os
from typing import Annotated, Optional, List

import jsonpatch
import jsonpointer
import typer
from cuid2 import Cuid
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table

from api.models import (
    DatabaseTreeResourceType, AddFormDTO, DatabaseTreeResourceVisibility,
    SchemaFieldDTO, FieldType, FieldTypeParametersUpdateDTO,
    TypeParameterLookupConfig, UpdateDatabaseDTO, FormSchema
)
from common import filter_data_forms, get_records_with_multiref, get_field_info, find_resource_by_prefix, \
    find_all_resources_by_prefix
from id_translation import SchemaIdTranslator
from utils import get_client, handle_api_errors, console

# Initialize a Typer sub-application for form and schema management
app = typer.Typer(no_args_is_help=True)

# Prefixes used to identify specific configuration forms within the ActivityInfo database
DATA_FORM_PREFIX = "0.1.2"
REFERENCE_FORM_PREFIX = "0.1.3"


def safe_apply_patch(patch_list: List[dict], schema_dict: dict) -> dict:
    """
    Apply a JSON patch list to a schema dict, skipping operations that fail 
    due to missing fields for remove, replace, or test operations.
    """
    patched_dict = schema_dict.copy()

    for op in patch_list:
        try:
            # Handle custom 'beforeCode' for semantic element positioning
            path = op.get("path", "")
            if op.get("op") == "add" and path.startswith("/elements/") and len(path.split("/")) == 3:
                field_code = jsonpointer.unescape(path.split("/")[2])
                elements = patched_dict.get("elements", {})
                
                if "beforeCode" in op:
                    before_code = op["beforeCode"]
                    value = op.get("value")
                    
                    if before_code is None:
                        # Insert at beginning
                        new_elements = {field_code: value}
                        new_elements.update(elements)
                        patched_dict["elements"] = new_elements
                    elif before_code in elements:
                        # Insert after before_code
                        new_elements = {}
                        for k, v in elements.items():
                            new_elements[k] = v
                            if k == before_code:
                                new_elements[field_code] = value
                        patched_dict["elements"] = new_elements
                    else:
                        # Append to end
                        elements[field_code] = value
                    continue

            # We apply one operation at a time to allow skipping failing ones
            p = jsonpatch.JsonPatch([op])
            patched_dict = p.apply(patched_dict)
        except (jsonpatch.JsonPatchException, jsonpointer.JsonPointerException, KeyError, IndexError):
            # If the operation fails, we check if it's one we can skip
            if op.get("op") in ("remove", "replace", "test"):
                # Specifically if it's about a field in elements
                path = op.get("path", "")
                if path.startswith("/elements/"):
                    # Skip silently for fields
                    continue
                # For other paths, we still skip as requested: "if it doesn't find a field to modify or delete it simply skips"
                continue
            else:
                # For 'add' or 'move' or 'copy', we might want to know if it fails, 
                # but the user said "simply skips and moves on"
                continue
    return patched_dict


@app.command(help="Create data forms from 0.1.2 in a given target database", no_args_is_help=True)
def create_data(
        target_database_id: Annotated[str, typer.Argument(help="The ActivityInfo ID of the target database")],
        root_folder_id: Annotated[
            Optional[str], typer.Argument(help="The root folder ID of the data folders (optional)")] = None,
        remove_forms: Annotated[
            bool, typer.Option(help="Remove existing forms missing from data form definitions")] = False,
        rebuild_forms: Annotated[
            bool, typer.Option(help="Fix existing forms to conform to the expected schema.")] = False
):
    """
    Automate the creation and maintenance of 'Data Forms' based on a central configuration.
    
    This command reads a 'Data Configuration' form (prefixed with 0.1.2) and ensures 
    that the target database has corresponding forms created with the correct 
    parent-child relationships and mandatory fields (Indicator, Project, etc.).
    """
    client = get_client()

    with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
    ) as progress:

        # --- 1. Initialization ---
        task = progress.add_task("Fetching database configuration...", total=None)

        # --- 2. Retrieve Target State ---
        # Fetch the database tree to understand the current structure and folders
        with handle_api_errors(f"Could not get tree for {target_database_id}"):
            target_tree = client.api.get_database_tree(target_database_id)

        # --- 3. Filter and Identify Existing Data Forms ---
        data_forms = filter_data_forms(target_tree, root_folder_id or target_database_id)
        data_forms_by_name = {f.label: f for f in data_forms}

        # --- 4. Locate and Read the Configuration Form ---
        # Find the form that contains the definitions for all data forms to be managed
        data_config_form = find_resource_by_prefix(target_tree.resources, DATA_FORM_PREFIX)
        if not data_config_form:
            progress.stop()
            console.print(
                f"[bold red]Error:[/bold red] Could not find data configuration form starting with {DATA_FORM_PREFIX}")
            raise typer.Exit(code=1)

        progress.update(task, description=f"Fetching records from {data_config_form.label}...")
        with handle_api_errors(f"Could not get records for {data_config_form.id}"):
            records = client.api.get_form(data_config_form.id)

        # --- 5. Iterate and Process each Form Definition ---
        progress.update(task, description="Processing forms...", total=len(records))
        cuid = Cuid(length=18)  # Used for generating unique, collision-resistant field/form IDs

        processed_sysnames = set()

        for record in records:
            form_name = record.get("SYSNAME")
            if not form_name:
                progress.advance(task)
                continue

            processed_sysnames.add(form_name)
            progress.update(task, description=f"Processing: {form_name}")

            existing_form_res = data_forms_by_name.get(form_name)

            # Skip processing if form exists and 'rebuild' flag isn't set
            if existing_form_res and not rebuild_forms:
                progress.advance(task)
                continue

            # --- Folder Determination Logic ---
            # Map the process and user level from the config to a specific folder in the target DB
            target_folder_prefix: Optional[str] = None
            if record["PROCESS.REFCODE"] == "PLAN":
                target_folder_prefix = "3" if record["USERLEVEL.REFCODE"] == "LC" else "4"
            elif record["PROCESS.REFCODE"] == "MNTR":
                target_folder_prefix = "6" if record["USERLEVEL.REFCODE"] == "LC" else "5"

            if target_folder_prefix is None:
                console.print(f"[yellow]Skipping {form_name}: Could not determine folder prefix.[/yellow]")
                progress.advance(task)
                continue

            # Locate the actual folder resource in the database tree
            parent_folder = find_resource_by_prefix(
                [res for res in target_tree.resources if res.type == DatabaseTreeResourceType.FOLDER and (
                        res.parentId == root_folder_id or res.parentId == target_database_id)],
                target_folder_prefix
            )

            if not parent_folder:
                console.print(f"[yellow]Skipping {form_name}: Target folder {target_folder_prefix} not found.[/yellow]")
                progress.advance(task)
                continue

            # --- Element Schema Construction ---
            # Dynamically build the list of fields (elements) for the form based on its type (IND, CSL, CST)
            elements: List[SchemaFieldDTO] = []

            def get_ref_form_id(prefix: str):
                """Helper to find IDs of reference forms based on their label prefix."""
                form = find_resource_by_prefix(
                    [res for res in target_tree.resources if res.type == DatabaseTreeResourceType.FORM],
                    prefix
                )
                if not form:
                    raise ValueError(f"Form with prefix {prefix} not found")
                return form.id

            # Add Project reference field if applicable
            if record["USERLEVEL.REFCODE"] == "LP":
                elements.append(SchemaFieldDTO(
                    code="PROJECT",
                    id=cuid.generate(),
                    key=True,
                    label="Project",
                    required=True,
                    type=FieldType.reference,
                    typeParameters=FieldTypeParametersUpdateDTO(
                        cardinality="single",
                        range=[{"formId": get_ref_form_id("2.2_")}],
                        lookupConfigs=[
                            TypeParameterLookupConfig(id=cuid.generate(), formula="LEADORG.GLOBORG.NAME",
                                                      lookupLabel="Lead Organization"),
                            TypeParameterLookupConfig(id=cuid.generate(), formula="REFLABEL", lookupLabel="Project")
                        ]
                    )
                ))

            # Add Indicator reference field if applicable
            if record["EFORM.REFCODE"] == "IND":
                elements.append(SchemaFieldDTO(
                    code="IND",
                    id=cuid.generate(),
                    key=True,
                    label="Indicator",
                    required=True,
                    type=FieldType.reference,
                    validationCondition="IND.ETYPE.REFCODE == \"CDE\" || !ISBLANK(SEARCH(\"|LC|\", IND.LFE.LFL.USERLEVEL_REFCODES))",
                    typeParameters=FieldTypeParametersUpdateDTO(
                        cardinality="single",
                        range=[{"formId": get_ref_form_id("1.3")}],
                        lookupConfigs=[
                            TypeParameterLookupConfig(id=cuid.generate(), formula="CDE.REFLABEL",
                                                      lookupLabel="Coordination Entity"),
                            TypeParameterLookupConfig(id=cuid.generate(), formula="LFE.REFLABEL",
                                                      lookupLabel="Logframe Entity"),
                            TypeParameterLookupConfig(id=cuid.generate(), formula="REFLABEL", lookupLabel="Indicator")
                        ]
                    )
                ))

            # Similar logic for Caseload and Cost Attachments...
            if record["EFORM.REFCODE"] == "CSL":
                elements.append(SchemaFieldDTO(
                    code="CSL",
                    id=cuid.generate(),
                    key=True,
                    label="Caseload Attachment",
                    required=True,
                    type=FieldType.reference,
                    validationCondition="CSL.ETYPE.REFCODE == \"CDE\" || !ISBLANK(SEARCH(\"|LC|\", CSL.LFE.LFL.USERLEVEL_REFCODES))",
                    typeParameters=FieldTypeParametersUpdateDTO(
                        cardinality="single",
                        range=[{"formId": get_ref_form_id("1.4")}],
                        lookupConfigs=[
                            TypeParameterLookupConfig(id=cuid.generate(), formula="CDE.REFLABEL",
                                                      lookupLabel="Coordination Entity"),
                            TypeParameterLookupConfig(id=cuid.generate(), formula="LFE.REFLABEL",
                                                      lookupLabel="Logframe Entity"),
                            TypeParameterLookupConfig(id=cuid.generate(), formula="REFLABEL",
                                                      lookupLabel="Caseload Attachment")
                        ]
                    )
                ))

            if record["EFORM.REFCODE"] == "CST":
                elements.append(SchemaFieldDTO(
                    code="CST",
                    id=cuid.generate(),
                    key=True,
                    label="Cost Attachment",
                    required=True,
                    type=FieldType.reference,
                    validationCondition="CST.ETYPE.REFCODE == \"CDE\" || !ISBLANK(SEARCH(\"|LC|\", CST.LFE.LFL.USERLEVEL_REFCODES))",
                    typeParameters=FieldTypeParametersUpdateDTO(
                        cardinality="single",
                        range=[{"formId": get_ref_form_id("1.5")}],
                        lookupConfigs=[
                            TypeParameterLookupConfig(id=cuid.generate(), formula="CDE.REFLABEL",
                                                      lookupLabel="Coordination Entity"),
                            TypeParameterLookupConfig(id=cuid.generate(), formula="LFE.REFLABEL",
                                                      lookupLabel="Logframe Entity"),
                            TypeParameterLookupConfig(id=cuid.generate(), formula="REFLABEL",
                                                      lookupLabel="Cost Attachment")
                        ]
                    )
                ))

            # --- Update or Create Form in Database ---
            if existing_form_res:
                # REBUILD: Merge our standardized fields with any custom fields already in the form
                with handle_api_errors(f"Could not rebuild form {form_name}"):
                    schema = client.api.get_form_schema(existing_form_res.id)

                    # ID preservation logic: ensure we don't change IDs of fields that match our codes
                    for new_elem in elements:
                        old_elem = next((e for e in schema.elements if e.code == new_elem.code), None)
                        if old_elem:
                            new_elem.id = old_elem.id
                            if new_elem.type_parameters and new_elem.type_parameters.lookup_configs and \
                                    old_elem.type_parameters and old_elem.type_parameters.lookup_configs:
                                for i, new_lc in enumerate(new_elem.type_parameters.lookup_configs):
                                    if i < len(old_elem.type_parameters.lookup_configs):
                                        new_lc.id = old_elem.type_parameters.lookup_configs[i].id

                    # Identify existing fields that are NOT part of our standard core fields
                    basic_codes_possible = {"PROJECT", "IND", "CSL", "CST"}
                    other_elements = [e for e in schema.elements if e.code not in basic_codes_possible]

                    # Concatenate standard core fields with existing custom ones
                    schema.elements = elements + other_elements
                    client.api.update_form_schema(schema)
            else:
                # CREATE: Define a brand new form structure
                form_id = cuid.generate()
                with handle_api_errors(f"Could not create form {form_name}"):
                    client.api.add_form(AddFormDTO(
                        formClass=AddFormDTO.FormClass(
                            databaseId=target_database_id,
                            id=form_id,
                            label=form_name,
                            schemaVersion=1,
                            parentFormId=None,
                            elements=elements,
                        ),
                        formResource=AddFormDTO.FormResource(
                            id=form_id,
                            label=form_name,
                            parentId=parent_folder.id,
                            type=DatabaseTreeResourceType.FORM,
                            visibility=DatabaseTreeResourceVisibility.PRIVATE,
                        )
                    ))

            progress.advance(task)

        # --- 6. Optional Cleanup ---
        # Remove forms from the target folder that are no longer present in the configuration
        extra_forms = [form for form in data_forms if form.label not in processed_sysnames]
        if remove_forms and extra_forms:
            progress.update(task, description="Removing extra forms...")
            extra_labels = [f.label for f in extra_forms]
            console.print(f"[yellow]Removing extra forms:[/yellow] {', '.join(extra_labels)}")
            with handle_api_errors("Could not delete extra forms"):
                client.api.update_database(target_database_id, UpdateDatabaseDTO(
                    resourceDeletions=[form.id for form in extra_forms],
                    resourceUpdates=[],
                    languageUpdates=[]
                ))

    console.print("[bold green]Creation process completed successfully.[/bold green]")


@app.command(help="Create reference forms from 0.1.3 in a given target database", no_args_is_help=True)
def create_reference(
        target_cm_database_id: Annotated[
            str, typer.Argument(help="The ActivityInfo ID of the target country module database")],
        grm_database_id: Annotated[
            str, typer.Argument(help="The ActivityInfo ID of the global reference module database")],
        remove_forms: Annotated[
            bool, typer.Option(help="Remove existing forms missing from reference form definitions")] = False,
        rebuild_forms: Annotated[
            bool, typer.Option(help="Fix existing forms to conform to the expected schema.")] = False
):
    """
    Synchronize 'Reference Forms' (Administrative levels, Sectors, etc.) from a Global Reference Module (GRM).
    
    This command follows a dependency-aware order to ensure that parent forms are created 
    before child forms. It maps global reference data to country-specific forms.
    """
    client = get_client()
    cuid = Cuid(length=18)

    with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
    ) as progress:
        task = progress.add_task("Fetching database configuration...", total=None)

        # Retrieve structural trees for both CM (Target) and GRM (Source)
        with handle_api_errors(f"Could not get tree for {target_cm_database_id}"):
            target_tree = client.api.get_database_tree(target_cm_database_id)

        # Identify the standard folder (prefixed '0.4') where reference forms reside
        parent_folder = find_resource_by_prefix(
            [res for res in target_tree.resources if res.type == DatabaseTreeResourceType.FOLDER],
            "0.4"
        )
        if not parent_folder:
            console.print("[bold red]Error:[/bold red] Could not find folder starting with '0.4' in target database.")
            raise typer.Exit(code=1)

        reference_forms_in_target = [
            res for res in target_tree.resources
            if res.type == DatabaseTreeResourceType.FORM and res.parentId == parent_folder.id
        ]
        reference_forms_by_name = {f.label: f for f in reference_forms_in_target}

        # Find the configuration form that defines which reference forms to sync
        reference_config_form = find_resource_by_prefix(
            [res for res in target_tree.resources if res.type == DatabaseTreeResourceType.FORM],
            REFERENCE_FORM_PREFIX
        )
        if not reference_config_form:
            console.print(
                f"[bold red]Error:[/bold red] Could not find reference configuration form {REFERENCE_FORM_PREFIX}")
            raise typer.Exit(code=1)

        progress.update(task, description="Fetching reference configuration records with multi-refs...")

        # Get the definitions including multi-value reference fields (Global forms to link)
        with handle_api_errors(f"Could not get records for {reference_config_form.id}"):
            records = get_records_with_multiref(client, reference_config_form.id)

        # --- 1. Dependency-Aware Sorting ---
        # Sort forms so that parents (referenced via PARENT_RFORM_REFCODE) are created first
        all_refcode_mans = {r.get("REFCODE_MAN") for r in records if r.get("REFCODE_MAN")}
        ordered_records = []
        processed_refcodes = set()
        remaining_records = records.copy()

        while remaining_records:
            made_progress = False
            for i in range(len(remaining_records) - 1, -1, -1):
                rec = remaining_records[i]
                parent_ref = rec.get("PARENT_RFORM_REFCODE")

                if not parent_ref or parent_ref not in all_refcode_mans or parent_ref in processed_refcodes:
                    ordered_records.append(remaining_records.pop(i))
                    if rec.get("REFCODE_MAN"):
                        processed_refcodes.add(rec.get("REFCODE_MAN"))
                    made_progress = True

            if not made_progress:
                # Break to prevent infinite loop in case of circular dependencies
                ordered_records.extend(remaining_records)
                break

        # --- 2. Iterate and Create/Update ---
        progress.update(task, description="Creating reference forms...", total=len(ordered_records))

        with handle_api_errors(f"Could not get tree for {grm_database_id}"):
            grm_tree = client.api.get_database_tree(grm_database_id)

        # Schema cache to minimize repetitive API calls
        schema_cache = {}

        def get_cached_schema(form_id):
            if form_id not in schema_cache:
                schema_cache[form_id] = client.api.get_form_schema(form_id)
            return schema_cache[form_id]

        created_forms_by_refcode_man = {}
        processed_sysnames = set()

        for rec in ordered_records:
            ref_code_man = rec.get("REFCODE_MAN")
            sys_name = rec.get("SYSNAME")
            def_refcode = rec.get("DEF.REFCODE")

            # Only process supported form definition types
            if def_refcode not in ["CMB", "SUB", "LCL"] or not sys_name:
                progress.advance(task)
                continue

            processed_sysnames.add(sys_name)
            existing = reference_forms_by_name.get(sys_name)
            if existing and not rebuild_forms:
                created_forms_by_refcode_man[ref_code_man] = existing.id
                progress.advance(task)
                continue

            elements = []

            # --- Logic for SUB (Sub-set) or CMB (Combined) forms ---
            # These link to one or more Global Reference forms
            if def_refcode in ["SUB", "CMB"]:
                glob_r_forms = rec.get("GLOBRFORMS", [])
                for x in glob_r_forms:
                    glob_sys_name = x.get("SYSNAME")
                    grm_form = next((f for f in grm_tree.resources if f.label == glob_sys_name), None)
                    if not grm_form:
                        console.print(
                            f"[yellow]Warning: GRM form {glob_sys_name} not found. Skipping {sys_name}[/yellow]")
                        continue

                    grm_schema = get_cached_schema(grm_form.id)
                    field_id, field_label = get_field_info(grm_schema)

                    # Add a reference field pointing to the global form
                    elements.append(SchemaFieldDTO(
                        code=x.get("REFCODE", cuid.generate()),
                        id=cuid.generate(),
                        key=True,
                        label=f"Equivalent Global {x.get('NAME', '')}",
                        required=True,
                        type=FieldType.reference,
                        typeParameters=FieldTypeParametersUpdateDTO(
                            cardinality="single",
                            range=[{"formId": grm_form.id}],
                            lookupConfigs=[
                                TypeParameterLookupConfig(
                                    id=cuid.generate(),
                                    formula=field_id,
                                    lookupLabel=field_label
                                )
                            ]
                        )
                    ))

            # --- Add standard REFCODE (Reference Code) field ---
            # For SUB/CMB, this is often a formula pulling from the global reference
            refcode_formula = None
            glob_r_forms = rec.get("GLOBRFORMS", [])
            if def_refcode == "SUB" and len(glob_r_forms) == 1:
                refcode_formula = f"{glob_r_forms[0].get('REFCODE')}.REFCODE"
            elif def_refcode == "CMB" and len(glob_r_forms) > 1:
                parts = [f"{x.get('REFCODE')}.REFCODE" for x in glob_r_forms]
                el = ', \"_\", '.join(parts)
                refcode_formula = f"CONCAT({el})"

            elements.append(SchemaFieldDTO(
                code="REFCODE",
                id=cuid.generate(),
                label="Reference Code",
                required=True,
                type=FieldType.FREE_TEXT,
                defaultValueFormula=refcode_formula,
                typeParameters=FieldTypeParametersUpdateDTO(barcode=False),
                readOnly=True if def_refcode == "SUB" else False,
                unique=True if def_refcode in ["SUB", "CMB"] else False,
                key=True if def_refcode == "LCL" else False
            ))

            # --- Add standard NAME field ---
            name_formula = None
            if def_refcode == "SUB" and len(glob_r_forms) == 1:
                name_formula = f"{glob_r_forms[0].get('REFCODE')}.NAME"
            elif def_refcode == "CMB" and len(glob_r_forms) > 1:
                parts = [f"{x.get('REFCODE')}.NAME" for x in glob_r_forms]
                el = ', \" \", '.join(parts)
                name_formula = f"CONCAT({el})"

            elements.append(SchemaFieldDTO(
                code="NAME",
                id=cuid.generate(),
                label="Name",
                required=True,
                type=FieldType.FREE_TEXT,
                defaultValueFormula=name_formula,
                typeParameters=FieldTypeParametersUpdateDTO(barcode=False),
                readOnly=True if def_refcode == "SUB" else False,
                unique=True
            ))

            # --- Logic for Hierarchical Parent Links ---
            parent_refcode = rec.get("PARENT_RFORM_REFCODE")
            if parent_refcode:
                parent_rec = next((r for r in records if r.get("REFCODE_MAN") == parent_refcode), None)
                parent_form_id = created_forms_by_refcode_man.get(parent_refcode)

                if parent_form_id:
                    parent_schema = get_cached_schema(parent_form_id)
                    p_field_id, p_field_label = get_field_info(parent_schema)

                    elements.append(SchemaFieldDTO(
                        code=parent_refcode,
                        id=cuid.generate(),
                        label=parent_rec.get("NAME") if parent_rec else "Parent",
                        required=True,
                        type=FieldType.reference,
                        typeParameters=FieldTypeParametersUpdateDTO(
                            cardinality="single",
                            range=[{"formId": parent_form_id}],
                            lookupConfigs=[
                                TypeParameterLookupConfig(
                                    id=cuid.generate(),
                                    formula=p_field_id,
                                    lookupLabel=p_field_label
                                )
                            ]
                        )
                    ))

            # --- Add standard REFLABEL (Display Label) field ---
            # This is a calculated field used as the primary label for the record
            reflabel_id = cuid.generate()
            elements.append(SchemaFieldDTO(
                code="REFLABEL",
                id=reflabel_id,
                label="Reference Label",
                required=False,
                type=FieldType.calculated,
                typeParameters=FieldTypeParametersUpdateDTO(
                    formula="CONCAT(REFCODE, \" - \", NAME)"
                ),
                dataEntryVisible=False,
                tableVisible=False
            ))

            # --- Update/Create Implementation ---
            if existing:
                with handle_api_errors(f"Could not rebuild form {sys_name}"):
                    schema = client.api.get_form_schema(existing.id)
                    reflabel_id = next((e.id for e in elements if e.code == "REFLABEL"), reflabel_id)

                    # Preservation of IDs for stability
                    for new_elem in elements:
                        old_elem = next((e for e in schema.elements if e.code == new_elem.code), None)
                        if old_elem:
                            new_elem.id = old_elem.id
                            if new_elem.type_parameters and new_elem.type_parameters.lookup_configs and \
                                    old_elem.type_parameters and old_elem.type_parameters.lookup_configs:
                                for i, new_lc in enumerate(new_elem.type_parameters.lookup_configs):
                                    if i < len(old_elem.type_parameters.lookup_configs):
                                        new_lc.id = old_elem.type_parameters.lookup_configs[i].id

                    schema.elements = elements
                    schema.record_label_field_id = reflabel_id
                    client.api.update_form_schema(schema)
                    created_forms_by_refcode_man[ref_code_man] = existing.id
            else:
                form_id = cuid.generate()
                with handle_api_errors(f"Could not create form {sys_name}"):
                    client.api.add_form(AddFormDTO(
                        formClass=AddFormDTO.FormClass(
                            databaseId=target_cm_database_id,
                            id=form_id,
                            parentFormId=None,
                            label=sys_name,
                            schemaVersion=1,
                            recordLabelFieldId=reflabel_id,
                            elements=elements,
                        ),
                        formResource=AddFormDTO.FormResource(
                            id=form_id,
                            label=sys_name,
                            parentId=parent_folder.id,
                            type=DatabaseTreeResourceType.FORM,
                            visibility=DatabaseTreeResourceVisibility.PRIVATE,
                        )
                    ))

                created_forms_by_refcode_man[ref_code_man] = form_id
            progress.advance(task)

        # --- 3. Cleanup ---
        extra_forms = [form for form in reference_forms_in_target if form.label not in processed_sysnames]
        if remove_forms and extra_forms:
            progress.update(task, description="Removing extra forms...")
            with handle_api_errors("Could not delete extra forms"):
                client.api.update_database(target_cm_database_id, UpdateDatabaseDTO(
                    resourceDeletions=[form.id for form in extra_forms],
                    resourceUpdates=[],
                    languageUpdates=[]
                ))

    console.print("[bold green]Reference creation process completed successfully.[/bold green]")


@app.command(help="Generate a form schema patch by comparing two versions of a form schema", no_args_is_help=True)
def patch(
        form_id: Annotated[Optional[str], typer.Argument(help="The ID of the form to patch")] = None,
        label: Annotated[Optional[str], typer.Option("--label", "-l", help="The label of the form to patch")] = None,
        database_id: Annotated[
            Optional[str], typer.Option("--db", "-d", help="The ID of the database (required if using label)")] = None,
):
    """
    Generate a JSON patch for a form schema.
    
    1. Identifies the form (by ID or by Label + Database ID).
    2. Fetches the current schema.
    3. Waits for the user to make changes in the ActivityInfo UI.
    4. Fetches the updated schema.
    5. Generates and saves a JSON patch between the two versions.
    """
    client = get_client()

    if not form_id:
        if not label or not database_id:
            console.print("[red]Error: You must provide either a form ID or both --label and --db.[/red]")
            raise typer.Exit(code=1)

        with handle_api_errors(f"Could not fetch tree for database {database_id}"):
            tree = client.api.get_database_tree(database_id)
            form_res = next(
                (res for res in tree.resources if res.type == DatabaseTreeResourceType.FORM and res.label == label),
                None)
            if not form_res:
                console.print(f"[red]Error: Form '{label}' not found in database {database_id}[/red]")
                raise typer.Exit(code=1)
            form_id = form_res.id

    with handle_api_errors(f"Could not fetch initial schema for form {form_id}"):
        console.print(f"[bold cyan]Fetching initial schema for form {form_id}...[/bold cyan]")
        schema1 = client.api.get_form_schema(form_id)
        schema1_dict = schema1.model_dump()

    console.print(f"\n[bold yellow]Initial schema captured for '{schema1.label}' ({form_id}).[/bold yellow]")
    console.print("Please go to the ActivityInfo UI and make your desired changes to the form schema.")

    if not typer.confirm("Have you finished making changes and want to generate the patch?"):
        console.print("[red]Operation cancelled.[/red]")
        raise typer.Abort()

    with handle_api_errors(f"Could not fetch updated schema for form {form_id}"):
        console.print(f"[bold cyan]Fetching updated schema for form {form_id}...[/bold cyan]")
        schema2 = client.api.get_form_schema(form_id)
        schema2_dict = schema2.model_dump()

    # Create mapping from ID to original semantic key (code or ID) for portable path translation
    id_to_key = {e["id"]: (e["code"] or e["id"]) for e in schema1_dict["elements"]}

    # Key both schemas by internal ID for stable identity-based diffing
    s1_id_keyed = schema1_dict.copy()
    s1_id_keyed["elements"] = {e["id"]: e for e in schema1_dict["elements"]}
    
    s2_id_keyed = schema2_dict.copy()
    s2_id_keyed["elements"] = {e["id"]: e for e in schema2_dict["elements"]}

    # Generate raw patch based on stable IDs
    raw_patch = jsonpatch.make_patch(s1_id_keyed, s2_id_keyed)
    
    # Transform paths from internal IDs back to portable semantic keys (codes)
    # and capture positional metadata for new elements.
    s2_ids = [e["id"] for e in schema2_dict["elements"]]
    patch_list = []
    for op in raw_patch:
        op_dict = op.copy()
        path = op_dict["path"]
        if path.startswith("/elements/"):
            parts = path.split("/")
            # parts[2] is the escaped field ID
            field_id = jsonpointer.unescape(parts[2])
            
            # Use original key if it existed, otherwise use new key (for 'add' operations)
            if field_id in id_to_key:
                key = id_to_key[field_id]
            else:
                new_element = s2_id_keyed["elements"].get(field_id)
                key = (new_element.get("code") or field_id) if new_element else field_id
            
            # Reconstruct path using the semantic key
            escaped_key = jsonpointer.escape(key)
            new_path = f"/elements/{escaped_key}"
            if len(parts) > 3:
                new_path += "/" + "/".join(parts[3:])
            op_dict["path"] = new_path

            # Add positional metadata for 'add' operations of full elements
            if op_dict["op"] == "add" and len(parts) == 3:
                try:
                    idx = s2_ids.index(field_id)
                    if idx > 0:
                        pred_id = s2_ids[idx-1]
                        # Capture the predecessor's code as the 'before' reference
                        pred_element = s2_id_keyed["elements"][pred_id]
                        op_dict["beforeCode"] = pred_element.get("code") or pred_id
                    else:
                        # idx == 0 means it should be at the very beginning
                        op_dict["beforeCode"] = None
                except ValueError:
                    pass

        patch_list.append(op_dict)

    # Sort the patch list to ensure that 'add' operations for elements follow 
    # the target schema order. This handles cases where one new field depends 
    # on another new field as its 'beforeCode'.
    def patch_sort_key(op):
        path = op.get("path", "")
        if op.get("op") == "add" and path.startswith("/elements/") and len(path.split("/")) == 3:
            field_code_or_id = jsonpointer.unescape(path.split("/")[2])
            # Find the ID associated with this code/ID in s2
            for eid, element in s2_id_keyed["elements"].items():
                if (element.get("code") or eid) == field_code_or_id:
                    return (0, s2_ids.index(eid))
        return (1, 0) # Non-add operations or other paths come after ordered adds

    patch_list.sort(key=patch_sort_key)

    if not patch_list:
        console.print("[yellow]No changes detected between the two schema versions.[/yellow]")
        return

    console.print("\n[bold green]Generated Semantic JSON Patch:[/bold green]")
    console.print_json(data=patch_list)

    payload = [{
        "form_id": form_id,
        "form_label": schema1.label,
        "patch": patch_list
    }]

    filename = f"form_patch_{form_id}.json"
    with open(filename, "w") as f:
        json.dump(payload, f, indent=2)

    console.print(f"\n[bold green]Patch saved to:[/bold green] {filename}")


@app.command(help="Apply a semantic JSON patch to forms in one or more target databases", no_args_is_help=True)
def apply(
        target_database_ids: Annotated[List[str], typer.Argument(help="The list of target database IDs")],
        patch_file: Annotated[
            str, typer.Option("--patch", "-p", help="Path to the JSON patch file")] = "form_patch.json",
        multi: Annotated[bool, typer.Option("--multi", "-m",
                                            help="Allow multiple form matches for each targeting technique")] = False,
        dry_run: Annotated[bool, typer.Option(help="Do not actually perform any changes")] = False,
        yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation prompt")] = False,
):
    """
    Apply a semantic JSON patch generated by the 'patch' command to one or more databases.
    
    This command:
    1. Loads the semantic patch (or array of patches) and metadata.
    2. Identifies target forms by ID, label, or prefix across databases.
    3. Fetches the current schema for each target form.
    4. Applies the patch(es) and translates internal IDs.
    5. Re-converts to list-based structure and pushes the update.
    """
    client = get_client()

    if not os.path.exists(patch_file):
        console.print(f"[red]Error: Patch file not found: {patch_file}[/red]")
        raise typer.Exit(code=1)

    with open(patch_file, "r") as f:
        patch_data = json.load(f)

    if isinstance(patch_data, list) and len(patch_data) > 0:
        if "op" in patch_data[0]:
            patch_entries = [{"patch": patch_data}]
        else:
            patch_entries = patch_data
    elif isinstance(patch_data, dict):
        if "patch" in patch_data:
            patch_entries = [patch_data]
        else:
            patch_entries = [{"patch": [patch_data]}]
    else:
        console.print(f"[red]Error: Invalid patch file format in {patch_file}[/red]")
        raise typer.Exit(code=1)

    def get_source_form_id(e):
        fid = e.get("form_id")
        if fid:
            return fid.split()[0] if isinstance(fid, str) else fid[0]

        patch_list = entry.get("patch", [])
        for op in patch_list:
            if op.get("path") == "/id" and isinstance(op.get("value"), str):
                return op.get("value")
        return None

    for entry in patch_entries:
        if not entry.get("form_label") and not entry.get("form_prefix") and not entry.get("form_id"):
            sid = get_source_form_id(entry)
            if sid:
                with handle_api_errors(f"Could not fetch label for source form {sid}"):
                    source_schema = client.api.get_form_schema(sid)
                    entry["form_label"] = source_schema.label

    def find_target_forms_in_db(tree, entry, multi_mode):
        forms = [res for res in tree.resources if res.type == DatabaseTreeResourceType.FORM]
        target_ids = set()
        results = []

        def add_resource(res):
            if res.id not in target_ids:
                target_ids.add(res.id)
                results.append(res)

        fid_spec = entry.get("form_id")
        if fid_spec:
            ids = fid_spec.split() if isinstance(fid_spec, str) else fid_spec
            for fid in ids:
                found = next((f for f in forms if f.id == fid), None)
                if found: add_resource(found)

        label = entry.get("form_label")
        if label:
            if multi_mode:
                for f in forms:
                    if f.label == label: add_resource(f)
            else:
                found = next((f for f in forms if f.label == label), None)
                if found: add_resource(found)

        prefix_spec = entry.get("form_prefix")
        if prefix_spec:
            prefixes = [prefix_spec] if isinstance(prefix_spec, str) else prefix_spec
            for prefix in prefixes:
                if multi_mode:
                    matches = find_all_resources_by_prefix(forms, prefix)
                    for m in matches: add_resource(m)
                else:
                    match = find_resource_by_prefix(forms, prefix)
                    if match: add_resource(match)

        return results

    planned_updates = []
    translators = {}

    def get_translator(source_form_id):
        if source_form_id not in translators:
            with handle_api_errors(f"Could not initialize translator for source form {source_form_id}"):
                translators[source_form_id] = SchemaIdTranslator(client, source_form_id)
        return translators[source_form_id]

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
            progress.update(task, description=f"Simulating patches for {db_id}...")
            with handle_api_errors(f"Could not simulate patches for {db_id}"):
                target_tree = client.api.get_database_tree(db_id)
                forms_in_db = [res for res in target_tree.resources if res.type == DatabaseTreeResourceType.FORM]

                # Group entries by target form
                form_to_entries = {f.id: [] for f in forms_in_db}
                for entry in patch_entries:
                    matched_forms = find_target_forms_in_db(target_tree, entry, multi)
                    for mf in matched_forms:
                        form_to_entries[mf.id].append(entry)

                # Process each form that has matching patches
                for form_res in forms_in_db:
                    entries_to_apply = form_to_entries.get(form_res.id, [])
                    if not entries_to_apply:
                        continue

                    # 1. Fetch current schema
                    schema = client.api.get_form_schema(form_res.id)
                    schema_dict = schema.model_dump()

                    # 2. Semantic conversion (list to dict keyed by code/id)
                    schema_dict["elements"] = {e["code"] or e["id"]: e for e in schema_dict["elements"]}
                    original_elements = schema_dict["elements"].copy()

                    current_schema_dict = schema_dict
                    unresolved_count = 0
                    unresolved_tokens = []

                    # 3. Apply all patches in sequence
                    for entry in entries_to_apply:
                        # Apply patch safely (skipping missing fields)
                        current_schema_dict = safe_apply_patch(entry.get("patch", []), current_schema_dict)

                        # Translate IDs if source context is available
                        sid = get_source_form_id(entry)
                        if sid:
                            translator = get_translator(sid)
                            report = translator.translate_schema(current_schema_dict, db_id)
                            current_schema_dict = report.translated_schema
                            unresolved_count += len(report.unresolved_source_form_ids) + len(
                                report.unresolved_source_field_ids)
                            unresolved_tokens.extend(
                                report.unresolved_source_form_ids + report.unresolved_source_field_ids)

                    # Ensure the ID and databaseId are preserved for the target database
                    current_schema_dict["id"] = form_res.id
                    current_schema_dict["databaseId"] = db_id

                    patched_elements = current_schema_dict.get("elements", {})
                    unresolved_preview = ", ".join(unresolved_tokens[:6]) if unresolved_count else "-"
                    if unresolved_count > 6:
                        unresolved_preview = f"{unresolved_preview}, ..."

                    planned_updates.append({
                        "db_id": db_id,
                        "form_id": form_res.id,
                        "form_label": schema.label,
                        "change_preview": build_compact_change_preview(original_elements, patched_elements),
                        "schema_preview": build_compact_schema_preview(current_schema_dict),
                        "unresolved_count": unresolved_count,
                        "unresolved_preview": unresolved_preview,
                        "patched_schema_dict": current_schema_dict,
                        "patches_applied": len(entries_to_apply)
                    })
            progress.advance(task)

    if not planned_updates:
        console.print("[yellow]No target forms found matching the patch criteria.[/yellow]")
        return

    # --- Preview Table ---
    table = Table(title=f"Patch Preview: {os.path.basename(patch_file)}")
    table.add_column("Database ID", style="cyan")
    table.add_column("Form ID", style="magenta")
    table.add_column("Form Label", style="white")
    table.add_column("Patches", style="blue")
    table.add_column("Unresolved IDs", style="red")
    table.add_column("Changes", style="yellow")
    table.add_column("Resulting Schema (Compact)", style="green")

    for up in planned_updates:
        table.add_row(
            up["db_id"],
            up["form_id"],
            up["form_label"],
            str(up["patches_applied"]),
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
                f"[red]- {up['db_id']} / {up['form_label']}[/red]: {up['unresolved_count']} unresolved token(s) "
                f"({up['unresolved_preview']})"
            )
        console.print("[dim]Hint: Ensure equivalent forms/fields exist in target DB (matching labels/codes).[/dim]")
        raise typer.Exit(code=1)

    if not yes and not typer.confirm("\nApply these patches to all listed forms?"):
        raise typer.Abort()

    # --- Execution ---
    with console.status("Applying patches...") as status:
        for up in planned_updates:
            status.update(f"Updating {up['form_label']} in {up['db_id']}...")
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
