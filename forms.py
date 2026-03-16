from typing import Annotated, Optional, List

import typer
from cuid2 import Cuid
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from api.models import (
    DatabaseTreeResourceType, AddFormDTO, DatabaseTreeResourceVisibility,
    SchemaFieldDTO, FieldType, FieldTypeParametersUpdateDTO,
    TypeParameterLookupConfig, UpdateDatabaseDTO
)
from common import filter_data_forms, get_records_with_multiref, get_field_info
from utils import get_client, handle_api_errors, console

# Initialize a Typer sub-application for form and schema management
app = typer.Typer(no_args_is_help=True)

# Prefixes used to identify specific configuration forms within the ActivityInfo database
DATA_FORM_PREFIX = "0.1.2"
REFERENCE_FORM_PREFIX = "0.1.3"


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
        data_config_form = next((res for res in target_tree.resources if res.label.startswith(DATA_FORM_PREFIX)), None)
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
            parent_folder = next(
                (res for res in target_tree.resources if res.type == DatabaseTreeResourceType.FOLDER and (
                        res.parentId == root_folder_id or res.parentId == target_database_id) and res.label.startswith(
                    target_folder_prefix)), None
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
                return next(res.id for res in target_tree.resources if
                            res.type == DatabaseTreeResourceType.FORM and res.label.startswith(prefix))

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
                        range=[{"formId": get_ref_form_id("2.2")}],
                        lookupConfigs=[
                            TypeParameterLookupConfig(id=cuid.generate(), formula="LEADORG.ORG.NAME",
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
        parent_folder = next(
            (res for res in target_tree.resources if
             res.type == DatabaseTreeResourceType.FOLDER and res.label.startswith("0.4")),
            None
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
        reference_config_form = next(
            (res for res in target_tree.resources if
             res.type == DatabaseTreeResourceType.FORM and res.label.startswith(REFERENCE_FORM_PREFIX)),
            None
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


if __name__ == "__main__":
    app()
