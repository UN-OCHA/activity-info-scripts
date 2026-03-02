import os
import pytest
from typer.testing import CliRunner
from api.models import (
    AddDatabaseDTO, AddFormDTO, DatabaseTreeResourceType, 
    DatabaseTreeResourceVisibility, SchemaFieldDTO, FieldType, 
    UpdateRecordsDTO, RecordUpdateDTO, Resource, UpdateDatabaseDTO,
    FieldTypeParametersUpdateDTO, TypeParameterLookupConfig
)
from forms import app
from cuid2 import Cuid

runner = CliRunner()

# =============================================================================
# 0.1.2 Data Forms Tests
# =============================================================================

@pytest.fixture
def forms_setup(api_client):
    cuid = Cuid(length=18)
    db_id = cuid.generate()
    api_client.api.add_database(
        AddDatabaseDTO(id=db_id, label="Forms Test DB", description="Testing forms", templateId="blank")
    )
    
    # Ensure original language is set
    api_client.api.update_database(db_id, UpdateDatabaseDTO(
        resourceUpdates=[],
        resourceDeletions=[],
        languageUpdates=[],
        originalLanguage="en"
    ))

    # Helper for simple forms
    def create_simple_form(label, code_field="REFCODE"):
        f_id = cuid.generate()
        api_client.api.add_form(AddFormDTO(
            formClass=AddFormDTO.FormClass(
                databaseId=db_id, id=f_id, label=label, schemaVersion=1, parentFormId=None,
                elements=[SchemaFieldDTO(id=cuid.generate(), code=code_field, label=label, required=True, type=FieldType.FREE_TEXT)]
            ),
            formResource=AddFormDTO.FormResource(
                id=f_id, label=label, parentId=db_id, type=DatabaseTreeResourceType.FORM, visibility=DatabaseTreeResourceVisibility.PRIVATE
            )
        ))
        return f_id

    # 1. Base Forms for 0.1.2 references
    proc_form_id = create_simple_form("Processes")
    ulevel_form_id = create_simple_form("UserLevels")
    eform_form_id = create_simple_form("EntityForms")

    # 2. Add records to base forms
    def add_ref_record(form_id, code_val):
        rec_id = cuid.generate()
        api_client.api.update_form_records([
            RecordUpdateDTO(formId=form_id, recordId=rec_id, fields={"REFCODE": code_val})
        ])
        return rec_id

    plan_rec_id = add_ref_record(proc_form_id, "PLAN")
    mntr_rec_id = add_ref_record(proc_form_id, "MNTR")
    lc_rec_id = add_ref_record(ulevel_form_id, "LC")
    lp_rec_id = add_ref_record(ulevel_form_id, "LP")
    ind_rec_id = add_ref_record(eform_form_id, "IND")
    csl_rec_id = add_ref_record(eform_form_id, "CSL")

    # 3. Create Folders
    folder_ids = {}
    resource_updates = []
    for i in ["3", "4", "5", "6"]:
        f_id = cuid.generate()
        resource_updates.append(Resource(
            id=f_id, parentId=db_id, label=f"{i}. Data Folder",
            type=DatabaseTreeResourceType.FOLDER, visibility=DatabaseTreeResourceVisibility.PRIVATE
        ))
        folder_ids[i] = f_id
    api_client.api.update_database(db_id, UpdateDatabaseDTO(resourceUpdates=resource_updates, originalLanguage="en"))

    # 4. Deep Hierarchy for Validation Formulas
    etype_form_id = create_simple_form("EntityTypes") 
    lfl_form_id = create_simple_form("LogframeLevels", "USERLEVEL_REFCODES") 
    org_form_id = create_simple_form("Organizations", "NAME") 

    # LogframeEntities
    lfe_form_id = cuid.generate()
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=db_id, id=lfe_form_id, label="LogframeEntities", schemaVersion=1,
            elements=[
                SchemaFieldDTO(id=cuid.generate(), code="LFL", label="Level", required=True, type=FieldType.reference, 
                               typeParameters=FieldTypeParametersUpdateDTO(cardinality="single", range=[{"formId": lfl_form_id}])),
                SchemaFieldDTO(id=cuid.generate(), code="REFLABEL", label="RefLabel", required=True, type=FieldType.FREE_TEXT)
            ]
        ),
        formResource=AddFormDTO.FormResource(id=lfe_form_id, label="LogframeEntities", parentId=db_id, type=DatabaseTreeResourceType.FORM)
    ))

    # LeadOrgs
    leadorg_form_id = cuid.generate()
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=db_id, id=leadorg_form_id, label="LeadOrgs", schemaVersion=1,
            elements=[
                SchemaFieldDTO(id=cuid.generate(), code="ORG", label="Org", required=True, type=FieldType.reference, 
                               typeParameters=FieldTypeParametersUpdateDTO(cardinality="single", range=[{"formId": org_form_id}]))
            ]
        ),
        formResource=AddFormDTO.FormResource(id=leadorg_form_id, label="LeadOrgs", parentId=db_id, type=DatabaseTreeResourceType.FORM)
    ))

    # 5. Reference forms 2.2, 1.3, 1.4, 1.5
    for prefix in ["2.2", "1.3", "1.4", "1.5"]:
        rf_id = cuid.generate()
        elements = [
            SchemaFieldDTO(id=cuid.generate(), code="REFLABEL", label="RefLabel", required=True, type=FieldType.FREE_TEXT),
            SchemaFieldDTO(id=cuid.generate(), code="NAME", label="Name", required=True, type=FieldType.FREE_TEXT)
        ]
        if prefix == "2.2":
            elements.append(SchemaFieldDTO(id=cuid.generate(), code="LEADORG", label="LeadOrg", required=True, type=FieldType.reference,
                                           typeParameters=FieldTypeParametersUpdateDTO(cardinality="single", range=[{"formId": leadorg_form_id}])))
        else:
            elements.extend([
                SchemaFieldDTO(id=cuid.generate(), code="ETYPE", label="EType", required=True, type=FieldType.reference,
                               typeParameters=FieldTypeParametersUpdateDTO(cardinality="single", range=[{"formId": etype_form_id}])),
                SchemaFieldDTO(id=cuid.generate(), code="LFE", label="LFE", required=True, type=FieldType.reference,
                               typeParameters=FieldTypeParametersUpdateDTO(cardinality="single", range=[{"formId": lfe_form_id}])),
                SchemaFieldDTO(id=cuid.generate(), code="CDE", label="CDE", required=True, type=FieldType.reference,
                               typeParameters=FieldTypeParametersUpdateDTO(cardinality="single", range=[{"formId": rf_id}]))
            ])

        api_client.api.add_form(AddFormDTO(
            formClass=AddFormDTO.FormClass(databaseId=db_id, id=rf_id, label=f"{prefix} Ref Form", schemaVersion=1, elements=elements),
            formResource=AddFormDTO.FormResource(id=rf_id, label=f"{prefix} Ref Form", parentId=db_id, type=DatabaseTreeResourceType.FORM)
        ))

    # 6. 0.1.2 Data Forms config form
    config_form_id = cuid.generate()
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=db_id, id=config_form_id, label="0.1.2 Data Forms", schemaVersion=1,
            elements=[
                SchemaFieldDTO(id=cuid.generate(), code="SYSNAME", label="System Name", required=True, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="PROCESS", label="Process", required=True, type=FieldType.reference,
                               typeParameters=FieldTypeParametersUpdateDTO(cardinality="single", range=[{"formId": proc_form_id}])),
                SchemaFieldDTO(id=cuid.generate(), code="USERLEVEL", label="User Level", required=True, type=FieldType.reference,
                               typeParameters=FieldTypeParametersUpdateDTO(cardinality="single", range=[{"formId": ulevel_form_id}])),
                SchemaFieldDTO(id=cuid.generate(), code="EFORM", label="Entity Form", required=True, type=FieldType.reference,
                               typeParameters=FieldTypeParametersUpdateDTO(cardinality="single", range=[{"formId": eform_form_id}])),
            ]
        ),
        formResource=AddFormDTO.FormResource(id=config_form_id, label="0.1.2 Data Forms", parentId=db_id, type=DatabaseTreeResourceType.FORM)
    ))

    # Wait for config form
    import time
    for _ in range(15):
        tree = api_client.api.get_database_tree(db_id)
        if any(r.id == config_form_id for r in tree.resources): break
        time.sleep(1)
    
    return db_id, config_form_id, folder_ids, {
        "PLAN": plan_rec_id, "MNTR": mntr_rec_id, "LC": lc_rec_id, "LP": lp_rec_id, "IND": ind_rec_id, "CSL": csl_rec_id
    }

def test_create_data_forms(api_client, ai_setup, forms_setup):
    db_id, config_form_id, folder_ids, rec_ids = forms_setup
    base_url = ai_setup["url"]
    token = ai_setup["token"]
    os.environ["API_TOKEN"] = token
    os.environ["ACTIVITYINFO_BASE_URL"] = f"{base_url}/resources/"

    # 1. Add records to 0.1.2
    api_client.api.update_form_records([
        RecordUpdateDTO(
            formId=config_form_id, recordId=Cuid(length=18).generate(),
            fields={"SYSNAME": "Test_Data_Form_3", "PROCESS": rec_ids["PLAN"], "USERLEVEL": rec_ids["LC"], "EFORM": rec_ids["IND"]}
        ),
        RecordUpdateDTO(
            formId=config_form_id, recordId=Cuid(length=18).generate(),
            fields={"SYSNAME": "Test_Data_Form_5", "PROCESS": rec_ids["MNTR"], "USERLEVEL": rec_ids["LP"], "EFORM": rec_ids["CSL"]}
        )
    ])

    # 2. Run create-data
    result = runner.invoke(app, ["create-data", db_id])
    if result.exit_code != 0:
        print(result.output)
    assert result.exit_code == 0
    
    # 3. Verify
    tree = api_client.api.get_database_tree(db_id)
    forms_created = {r.label: r for r in tree.resources if r.type == DatabaseTreeResourceType.FORM}
    assert "Test_Data_Form_3" in forms_created
    assert forms_created["Test_Data_Form_3"].parentId == folder_ids["3"]
    assert "Test_Data_Form_5" in forms_created
    assert forms_created["Test_Data_Form_5"].parentId == folder_ids["5"]
    
    # 4. Verify elements
    schema3 = api_client.api.get_form_schema(forms_created["Test_Data_Form_3"].id)
    codes3 = [e.code for e in schema3.elements]
    assert "IND" in codes3
    
    schema5 = api_client.api.get_form_schema(forms_created["Test_Data_Form_5"].id)
    codes5 = [e.code for e in schema5.elements]
    assert "PROJECT" in codes5
    assert "CSL" in codes5

    # 5. Test Rebuild
    records = api_client.api.get_form(config_form_id)
    rec5 = next(r for r in records if r["SYSNAME"] == "Test_Data_Form_5")
    api_client.api.update_form_records([
        RecordUpdateDTO(formId=config_form_id, recordId=rec5["@id"], fields={"EFORM": rec_ids["IND"]})
    ])
    result = runner.invoke(app, ["create-data", db_id, "--rebuild-forms"])
    assert result.exit_code == 0
    schema5_new = api_client.api.get_form_schema(forms_created["Test_Data_Form_5"].id)
    codes5_new = [e.code for e in schema5_new.elements]
    assert "IND" in codes5_new
    assert "CSL" not in codes5_new
    
    # 6. Test Remove
    rec3 = next(r for r in records if r["SYSNAME"] == "Test_Data_Form_3")
    api_client.api.update_form_records([
        RecordUpdateDTO(formId=config_form_id, recordId=rec3["@id"], deleted=True, fields={})
    ])
    result = runner.invoke(app, ["create-data", db_id, "--remove-forms"])
    assert result.exit_code == 0
    tree_final = api_client.api.get_database_tree(db_id)
    labels_final = [r.label for r in tree_final.resources]
    assert "Test_Data_Form_3" not in labels_final
    assert "Test_Data_Form_5" in labels_final


# =============================================================================
# 0.1.3 Reference Forms Tests
# =============================================================================

@pytest.fixture
def reference_setup(api_client):
    cuid = Cuid(length=18)
    
    # 1. Setup GRM Database
    grm_id = cuid.generate()
    api_client.api.add_database(AddDatabaseDTO(id=grm_id, label="GRM DB", description="Global Reference Module", templateId="blank"))
    
    def create_grm_form(label, code):
        f_id = cuid.generate()
        reflabel_id = cuid.generate()
        api_client.api.add_form(AddFormDTO(
            formClass=AddFormDTO.FormClass(
                databaseId=grm_id, id=f_id, label=label, schemaVersion=1, parentFormId=None,
                recordLabelFieldId=reflabel_id,
                elements=[
                    SchemaFieldDTO(id=cuid.generate(), code="REFCODE", label="Code", required=True, type=FieldType.FREE_TEXT),
                    SchemaFieldDTO(id=cuid.generate(), code="NAME", label="Name", required=True, type=FieldType.FREE_TEXT),
                    SchemaFieldDTO(id=reflabel_id, code="REFLABEL", label="RefLabel", required=False, type=FieldType.calculated, 
                                   typeParameters=FieldTypeParametersUpdateDTO(formula='CONCAT(REFCODE, " - ", NAME)'))
                ]
            ),
            formResource=AddFormDTO.FormResource(id=f_id, label=label, parentId=grm_id, type=DatabaseTreeResourceType.FORM)
        ))
        return f_id

    age_form_grm = create_grm_form("Global_Ages", "GLOBAGE")
    sex_form_grm = create_grm_form("Global_Sexes", "GLOBSEX")

    # We need a form in GRM that LISTS forms, so GLOBRFORMS can reference it.
    grm_list_id = cuid.generate()
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=grm_id, id=grm_list_id, label="Global Forms List", schemaVersion=1,
            elements=[
                SchemaFieldDTO(id=cuid.generate(), code="REFCODE", label="RefCode", required=True, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="NAME", label="Name", required=True, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="SYSNAME", label="SysName", required=True, type=FieldType.FREE_TEXT),
            ]
        ),
        formResource=AddFormDTO.FormResource(id=grm_list_id, label="Global Forms List", parentId=grm_id, type=DatabaseTreeResourceType.FORM)
    ))

    # 2. Setup Target Database
    target_id = cuid.generate()
    api_client.api.add_database(AddDatabaseDTO(id=target_id, label="Target CM DB", description="Country Module", templateId="blank"))
    
    # Create Folder 0.4
    folder_04_id = cuid.generate()
    api_client.api.update_database(target_id, UpdateDatabaseDTO(
        resourceUpdates=[Resource(id=folder_04_id, parentId=target_id, label="0.4 Reference", type=DatabaseTreeResourceType.FOLDER)],
        originalLanguage="en"
    ))

    # Create Reference Types Form (for DEF.REFCODE)
    reftype_form_id = cuid.generate()
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=target_id, id=reftype_form_id, label="Reference Types", schemaVersion=1,
            elements=[SchemaFieldDTO(id=cuid.generate(), code="REFCODE", label="Code", required=True, type=FieldType.FREE_TEXT)]
        ),
        formResource=AddFormDTO.FormResource(id=reftype_form_id, label="Reference Types", parentId=target_id, type=DatabaseTreeResourceType.FORM)
    ))
    
    # Add SUB, CMB, LCL records
    def add_type(code):
        rid = cuid.generate()
        api_client.api.update_form_records([RecordUpdateDTO(formId=reftype_form_id, recordId=rid, fields={"REFCODE": code})])
        return rid
    
    sub_rid = add_type("SUB")
    cmb_rid = add_type("CMB")
    lcl_rid = add_type("LCL")

    # Create 0.1.3 Reference Forms config form
    config_form_id = cuid.generate()
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=target_id, id=config_form_id, label="0.1.3 Reference Forms", schemaVersion=1,
            elements=[
                SchemaFieldDTO(id=cuid.generate(), code="DEF", label="Def", required=True, type=FieldType.reference,
                               typeParameters=FieldTypeParametersUpdateDTO(cardinality="single", range=[{"formId": reftype_form_id}])),
                SchemaFieldDTO(id=cuid.generate(), code="GLOBRFORMS", label="Global Reference Forms", required=False, type=FieldType.reference,
                               typeParameters=FieldTypeParametersUpdateDTO(cardinality="multiple", range=[{"formId": grm_list_id}])),
                SchemaFieldDTO(id=cuid.generate(), code="REFCODE_MAN", label="RefCode Man", required=True, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="NAME", label="Name", required=True, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="PARENT_RFORM_REFCODE", label="Parent Ref", required=False, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="SYSNAME", label="SysName", required=True, type=FieldType.FREE_TEXT),
            ]
        ),
        formResource=AddFormDTO.FormResource(id=config_form_id, label="0.1.3 Reference Forms", parentId=target_id, type=DatabaseTreeResourceType.FORM)
    ))

    # Add records to Global Forms List
    def add_grm_list_rec(ref, name, sysname):
        rid = cuid.generate()
        api_client.api.update_form_records([RecordUpdateDTO(formId=grm_list_id, recordId=rid, fields={"REFCODE": ref, "NAME": name, "SYSNAME": sysname})])
        return rid
    
    age_grm_rid = add_grm_list_rec("GLOBAGE", "Age", "Global_Ages")
    sex_grm_rid = add_grm_list_rec("GLOBSEX", "Sex", "Global_Sexes")

    # Wait for everything
    import time
    for _ in range(15):
        tree = api_client.api.get_database_tree(target_id)
        if any(r.id == config_form_id for r in tree.resources): break
        time.sleep(1)
    
    return target_id, grm_id, config_form_id, folder_04_id, {
        "SUB": sub_rid, "CMB": cmb_rid, "LCL": lcl_rid,
        "AGE_GRM": age_grm_rid, "SEX_GRM": sex_grm_rid
    }

def test_create_reference_forms(api_client, ai_setup, reference_setup):
    target_id, grm_id, config_id, folder_04_id, rids = reference_setup
    base_url = ai_setup["url"]
    token = ai_setup["token"]
    os.environ["API_TOKEN"] = token
    os.environ["ACTIVITYINFO_BASE_URL"] = f"{base_url}/resources/"

    # 1. Add records to 0.1.3
    api_client.api.update_form_records([
        # SUB: Ages
        RecordUpdateDTO(
            formId=config_id, recordId=Cuid(length=18).generate(),
            fields={
                "DEF": rids["SUB"],
                "GLOBRFORMS": rids["AGE_GRM"],
                "REFCODE_MAN": "AGE",
                "NAME": "Age",
                "SYSNAME": "Operation_Ages"
            }
        ),
        # CMB: Age & Sex
        RecordUpdateDTO(
            formId=config_id, recordId=Cuid(length=18).generate(),
            fields={
                "DEF": rids["CMB"],
                "GLOBRFORMS": f"{rids['AGE_GRM']}, {rids['SEX_GRM']}",
                "REFCODE_MAN": "AGE_SEX",
                "NAME": "Age & Sex",
                "SYSNAME": "Operation_Combination_Ages_Sexes"
            }
        ),
        # LCL: Admin0
        RecordUpdateDTO(
            formId=config_id, recordId=Cuid(length=18).generate(),
            fields={
                "DEF": rids["LCL"],
                "REFCODE_MAN": "ADMIN0",
                "NAME": "Country",
                "SYSNAME": "Operation_Countries"
            }
        ),
        # LCL: Admin1 (Child of Admin0)
        RecordUpdateDTO(
            formId=config_id, recordId=Cuid(length=18).generate(),
            fields={
                "DEF": rids["LCL"],
                "REFCODE_MAN": "ADMIN1",
                "NAME": "Province",
                "PARENT_RFORM_REFCODE": "ADMIN0",
                "SYSNAME": "Operation_Provinces"
            }
        )
    ])

    # 2. Run create-reference
    result = runner.invoke(app, ["create-reference", target_id, grm_id])
    if result.exit_code != 0:
        print(result.output)
    assert result.exit_code == 0
    
    # 3. Verify forms created in folder 0.4
    tree = api_client.api.get_database_tree(target_id)
    refs_created = {r.label: r for r in tree.resources if r.parentId == folder_04_id}
    
    assert "Operation_Ages" in refs_created
    assert "Operation_Combination_Ages_Sexes" in refs_created
    assert "Operation_Countries" in refs_created
    assert "Operation_Provinces" in refs_created
    
    # 4. Verify SUB Form Schema
    schema_age = api_client.api.get_form_schema(refs_created["Operation_Ages"].id)
    codes_age = {e.code: e for e in schema_age.elements}
    assert "GLOBAGE" in codes_age
    assert codes_age["REFCODE"].default_value_formula == "GLOBAGE.REFCODE"
    assert codes_age["NAME"].default_value_formula == "GLOBAGE.NAME"
    
    # 5. Verify CMB Form Schema
    schema_cmb = api_client.api.get_form_schema(refs_created["Operation_Combination_Ages_Sexes"].id)
    codes_cmb = {e.code: e for e in schema_cmb.elements}
    assert "GLOBAGE" in codes_cmb
    assert "GLOBSEX" in codes_cmb
    assert 'CONCAT(GLOBAGE.REFCODE, "_", GLOBSEX.REFCODE)' in codes_cmb["REFCODE"].default_value_formula
    assert 'CONCAT(GLOBAGE.NAME, " ", GLOBSEX.NAME)' in codes_cmb["NAME"].default_value_formula

    # 6. Verify Parent Ref in LCL
    schema_p = api_client.api.get_form_schema(refs_created["Operation_Provinces"].id)
    codes_p = {e.code: e for e in schema_p.elements}
    assert "ADMIN0" in codes_p
    assert codes_p["ADMIN0"].type == FieldType.reference
    assert codes_p["ADMIN0"].type_parameters.range[0]["formId"] == refs_created["Operation_Countries"].id
