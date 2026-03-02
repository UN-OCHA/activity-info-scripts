from typing import Annotated, Optional, List, Dict

import typer
from cuid2 import Cuid
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from api.models import (
    DatabaseTreeResourceType, AddFormDTO, DatabaseTreeResourceVisibility,
    SchemaFieldDTO, FieldType, FieldTypeParametersUpdateDTO,
    TypeParameterLookupConfig, UpdateDatabaseDTO, FormSchema
)
from common import filter_data_forms
from utils import get_client, handle_api_errors, console

app = typer.Typer(no_args_is_help=True)

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
    client = get_client()

    with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
    ) as progress:

        # 1. Initialize Task
        task = progress.add_task("Fetching database configuration...", total=None)

        # 2. Get the target DB's tree
        with handle_api_errors(f"Could not get tree for {target_database_id}"):
            target_tree = client.api.get_database_tree(target_database_id)

        # 3. Filter data forms
        data_forms = filter_data_forms(target_tree, root_folder_id or target_database_id)
        data_forms_by_name = {f.label: f for f in data_forms}

        # 4. Get data form records
        data_config_form = next((res for res in target_tree.resources if res.label.startswith(DATA_FORM_PREFIX)), None)
        if not data_config_form:
            progress.stop()
            console.print(
                f"[bold red]Error:[/bold red] Could not find data configuration form starting with {DATA_FORM_PREFIX}")
            raise typer.Exit(code=1)

        progress.update(task, description=f"Fetching records from {data_config_form.label}...")
        with handle_api_errors(f"Could not get records for {data_config_form.id}"):
            records = client.api.get_form(data_config_form.id)

        # 5. Process records
        progress.update(task, description="Processing forms...", total=len(records))
        cuid = Cuid(length=18)

        processed_sysnames = set()

        for record in records:
            form_name = record.get("SYSNAME")
            if not form_name:
                progress.advance(task)
                continue

            processed_sysnames.add(form_name)
            progress.update(task, description=f"Processing: {form_name}")

            existing_form_res = data_forms_by_name.get(form_name)

            if existing_form_res and not rebuild_forms:
                progress.advance(task)
                continue

            # Determine target folder
            target_folder_prefix: Optional[str] = None
            if record["PROCESS.REFCODE"] == "PLAN":
                target_folder_prefix = "3" if record["USERLEVEL.REFCODE"] == "LC" else "4"
            elif record["PROCESS.REFCODE"] == "MNTR":
                target_folder_prefix = "6" if record["USERLEVEL.REFCODE"] == "LC" else "5"

            if target_folder_prefix is None:
                console.print(f"[yellow]Skipping {form_name}: Could not determine folder prefix.[/yellow]")
                progress.advance(task)
                continue

            # Find target folder resource
            parent_folder = next(
                (res for res in target_tree.resources if res.type == DatabaseTreeResourceType.FOLDER and (
                        res.parentId == root_folder_id or res.parentId == target_database_id) and res.label.startswith(
                    target_folder_prefix)), None
            )

            if not parent_folder:
                console.print(f"[yellow]Skipping {form_name}: Target folder {target_folder_prefix} not found.[/yellow]")
                progress.advance(task)
                continue

            # Build elements list
            elements: List[SchemaFieldDTO] = []

            def get_ref_form_id(prefix: str):
                return next(res.id for res in target_tree.resources if
                            res.type == DatabaseTreeResourceType.FORM and res.label.startswith(prefix))

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

            if existing_form_res:
                # Rebuild existing form
                with handle_api_errors(f"Could not rebuild form {form_name}"):
                    schema = client.api.get_form_schema(existing_form_res.id)

                    # Preserve IDs for matching codes
                    for new_elem in elements:
                        old_elem = next((e for e in schema.elements if e.code == new_elem.code), None)
                        if old_elem:
                            new_elem.id = old_elem.id
                            # Also try to preserve lookupConfig IDs if possible
                            if new_elem.type_parameters and new_elem.type_parameters.lookup_configs and \
                                    old_elem.type_parameters and old_elem.type_parameters.lookup_configs:
                                for i, new_lc in enumerate(new_elem.type_parameters.lookup_configs):
                                    if i < len(old_elem.type_parameters.lookup_configs):
                                        new_lc.id = old_elem.type_parameters.lookup_configs[i].id

                    # Identify other elements (metrics, disaggs, etc. added by other scripts)
                    # We identify them by the fact that their codes are NOT in the list of basic field codes
                    # that CAN BE in a 0.1.2 form (PROJECT, IND, CSL, CST).
                    basic_codes_possible = {"PROJECT", "IND", "CSL", "CST"}
                    other_elements = [e for e in schema.elements if e.code not in basic_codes_possible]

                    schema.elements = elements + other_elements
                    client.api.update_form_schema(schema)
            else:
                # Create new form
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

        # 6. Cleanup
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


def get_records_with_multiref(client, form_id: str):
    base_records = client.api.get_form(form_id)
    schema = client.api.get_form_schema(form_id)

    multiref_fields = [
        field for field in schema.elements
        if field.type == FieldType.multiselectreference
    ]

    for field in multiref_fields:
        if not field.type_parameters or not field.type_parameters.range:
            continue

        ref_form_id = field.type_parameters.range[0]["formId"]
        ref_records = client.api.get_form(ref_form_id)
        ref_records_map = {rec["@id"]: rec for rec in ref_records}

        field_id_key = f"{field.code}.@id"

        for record in base_records:
            # The API returns multi-ref IDs as a comma-separated string in "CODE.@id"
            ids_str = record.get(field_id_key)
            if ids_str:
                ids = [i.strip() for i in ids_str.split(",")]
                record[field.code] = [
                    ref_records_map[i] for i in ids if i in ref_records_map
                ]
            else:
                record[field.code] = []

    return base_records


def get_field_info(schema: FormSchema):
    # Spec: get label and id of the element:
    # The element with id = recordLabelFieldId
    if schema.record_label_field_id:
        field = next((f for f in schema.elements if f.id == schema.record_label_field_id), None)
        if field:
            return field.id, field.label

    # Spec: if not, then the element with code 'REFLABEL'
    # if not, then the element with code 'NAME'
    # if not, then the first element
    for code in ["REFLABEL", "NAME"]:
        field = next((f for f in schema.elements if f.code == code), None)
        if field:
            return field.id, field.label

    if schema.elements:
        return schema.elements[0].id, schema.elements[0].label
    return None, None


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

        with handle_api_errors(f"Could not get tree for {target_cm_database_id}"):
            target_tree = client.api.get_database_tree(target_cm_database_id)

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

        with handle_api_errors(f"Could not get records for {reference_config_form.id}"):
            records = get_records_with_multiref(client, reference_config_form.id)

        # 1. Determine processing order
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
                ordered_records.extend(remaining_records)
                break

        # 2. Iterate and create
        progress.update(task, description="Creating reference forms...", total=len(ordered_records))

        with handle_api_errors(f"Could not get tree for {grm_database_id}"):
            grm_tree = client.api.get_database_tree(grm_database_id)

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

            # SUB or CMB logic
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

            # REFCODE field
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

            # NAME field
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

            # PARENT_RFORM_REFCODE logic (ADMIN0)
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

            # REFLABEL field
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

            if existing:
                # Rebuild existing form
                with handle_api_errors(f"Could not rebuild form {sys_name}"):
                    schema = client.api.get_form_schema(existing.id)
                    reflabel_id = next((e.id for e in elements if e.code == "REFLABEL"), reflabel_id)

                    # Preserve IDs
                    for new_elem in elements:
                        old_elem = next((e for e in schema.elements if e.code == new_elem.code), None)
                        if old_elem:
                            new_elem.id = old_elem.id
                            if new_elem.type_parameters and new_elem.type_parameters.lookup_configs and \
                                    old_elem.type_parameters and old_elem.type_parameters.lookup_configs:
                                for i, new_lc in enumerate(new_elem.type_parameters.lookup_configs):
                                    if i < len(old_elem.type_parameters.lookup_configs):
                                        new_lc.id = old_elem.type_parameters.lookup_configs[i].id

                    schema.elements = elements # In Ref forms, we usually want exactly these fields
                    schema.record_label_field_id = reflabel_id
                    client.api.update_form_schema(schema)
                    created_forms_by_refcode_man[ref_code_man] = existing.id
            else:
                # Finalize Form Creation
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

        # 3. Cleanup
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
