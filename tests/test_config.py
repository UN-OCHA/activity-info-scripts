import os

import pytest
from cuid2 import Cuid
from typer.testing import CliRunner

from api.models import (
    AddDatabaseDTO, AddFormDTO, DatabaseTreeResourceType,
    SchemaFieldDTO, FieldType,
    RecordUpdateDTO, UpdateDatabaseDTO, Resource, FieldTypeParametersUpdateDTO
)
from config import app

runner = CliRunner()


@pytest.fixture
def metric_setup(api_client):
    cuid = Cuid(length=18)
    db_id = cuid.generate()
    api_client.api.add_database(
        AddDatabaseDTO(id=db_id, label="Metric Test DB", description="Testing metrics", templateId="blank")
    )

    api_client.api.update_database(db_id, UpdateDatabaseDTO(
        resourceUpdates=[], resourceDeletions=[], languageUpdates=[], originalLanguage="en"
    ))

    # Create Folder 3
    folder_id = cuid.generate()
    api_client.api.update_database(db_id, UpdateDatabaseDTO(
        resourceUpdates=[Resource(id=folder_id, parentId=db_id, label="3. Data", type=DatabaseTreeResourceType.FOLDER)],
        originalLanguage="en"
    ))

    # Create a dummy reference form for CSL to reference
    ref_id = cuid.generate()
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=db_id, id=ref_id, label="Ref_CSL", schemaVersion=1,
            elements=[SchemaFieldDTO(id=cuid.generate(), code="REFLABEL", label="Label", required=True,
                                     type=FieldType.FREE_TEXT)]
        ),
        formResource=AddFormDTO.FormResource(id=ref_id, label="Ref_CSL", parentId=db_id,
                                             type=DatabaseTreeResourceType.FORM)
    ))

    # Create a Data Form
    data_form_id = cuid.generate()
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=db_id, id=data_form_id, label="Test_Data_Form", schemaVersion=1,
            elements=[
                SchemaFieldDTO(id=cuid.generate(), code="PROJ", label="Project", required=True,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="CSL", label="Caseload", required=True,
                               type=FieldType.reference,
                               typeParameters=FieldTypeParametersUpdateDTO(cardinality="single",
                                                                           range=[{"formId": ref_id}])),
                SchemaFieldDTO(id=cuid.generate(), code="OTHER", label="Other Field", required=False,
                               type=FieldType.FREE_TEXT),
            ]
        ),
        formResource=AddFormDTO.FormResource(
            id=data_form_id, label="Test_Data_Form", parentId=folder_id, type=DatabaseTreeResourceType.FORM
        )
    ))

    # Create 0.3.3 Metric Configuration Form
    metric_config_id = cuid.generate()
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=db_id, id=metric_config_id, label="0.3.3 Metric Configuration", schemaVersion=1,
            elements=[
                SchemaFieldDTO(id=cuid.generate(), code="DFORM_SYSNAME", label="Form Name", required=True,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="REFORDER", label="Order", required=True,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="DISPLAY_REFCODE", label="Display", required=True,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="NAME", label="Name", required=True, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="REFCODE_MAN", label="RefCode Man", required=False,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="REFCODE", label="RefCode", required=True,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="CCODE", label="C-Code", required=True,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="EFORM_REFCODE", label="E-Form RefCode", required=True,
                               type=FieldType.FREE_TEXT),
            ]
        ),
        formResource=AddFormDTO.FormResource(
            id=metric_config_id, label="0.3.3 Metric Configuration", parentId=db_id, type=DatabaseTreeResourceType.FORM
        )
    ))

    return db_id, data_form_id, metric_config_id


def test_metric_adjustment(api_client, ai_setup, metric_setup):
    db_id, data_form_id, metric_config_id = metric_setup
    base_url = ai_setup["url"]
    token = ai_setup["token"]
    os.environ["API_TOKEN"] = token
    os.environ["ACTIVITYINFO_BASE_URL"] = f"{base_url}/resources/"

    api_client.api.update_form_records([
        RecordUpdateDTO(
            formId=metric_config_id, recordId="recmetric",
            fields={
                "DFORM_SYSNAME": "Test_Data_Form", "REFORDER": "01", "DISPLAY_REFCODE": "MAN",
                "NAME": "Severity Score", "REFCODE": "AMOUNT_SEV", "CCODE": "DATA_SEV", "EFORM_REFCODE": "CSL"
            }
        )
    ])

    result = runner.invoke(app, ["metric", db_id])
    assert result.exit_code == 0

    schema = api_client.api.get_form_schema(data_form_id)
    codes = [e.code for e in schema.elements]
    assert "AMOUNT_SEV_MAN" in codes
    assert "AMOUNT_SEV" in codes

    # Check order: should be after CSL
    csl_idx = codes.index("CSL")
    assert codes[csl_idx + 1] == "AMOUNT_SEV_MAN"


def test_metric_rebuild(api_client, ai_setup, metric_setup):
    db_id, data_form_id, metric_config_id = metric_setup
    base_url = ai_setup["url"]
    token = ai_setup["token"]
    os.environ["API_TOKEN"] = token
    os.environ["ACTIVITYINFO_BASE_URL"] = f"{base_url}/resources/"

    api_client.api.update_form_records([
        RecordUpdateDTO(
            formId=metric_config_id, recordId="recmone",
            fields={
                "DFORM_SYSNAME": "Test_Data_Form", "REFORDER": "01", "DISPLAY_REFCODE": "MAN",
                "NAME": "Severity Score", "REFCODE": "AMOUNT_SEV", "CCODE": "DATA_SEV", "EFORM_REFCODE": "CSL"
            }
        )
    ])
    runner.invoke(app, ["metric", db_id])

    api_client.api.update_form_records([
        RecordUpdateDTO(formId=metric_config_id, recordId="recmone", fields={"DISPLAY_REFCODE": "CALC"})
    ])
    runner.invoke(app, ["metric", db_id, "--rebuild-fields"])
    schema = api_client.api.get_form_schema(data_form_id)
    codes = [e.code for e in schema.elements]
    assert "AMOUNT_SEV_MAN" not in codes
    assert "AMOUNT_SEV" in codes


def test_metric_remove(api_client, ai_setup, metric_setup):
    db_id, data_form_id, metric_config_id = metric_setup
    base_url = ai_setup["url"]
    token = ai_setup["token"]
    os.environ["API_TOKEN"] = token
    os.environ["ACTIVITYINFO_BASE_URL"] = f"{base_url}/resources/"

    api_client.api.update_form_records([
        RecordUpdateDTO(
            formId=metric_config_id, recordId="recmr",
            fields={
                "DFORM_SYSNAME": "Test_Data_Form", "REFORDER": "01", "DISPLAY_REFCODE": "MAN",
                "NAME": "Severity Score", "REFCODE": "AMOUNT_SEV", "CCODE": "DATA_SEV", "EFORM_REFCODE": "CSL"
            }
        )
    ])
    runner.invoke(app, ["metric", db_id])
    api_client.api.update_form_records(
        [RecordUpdateDTO(formId=metric_config_id, recordId="recmr", deleted=True, fields={})])
    runner.invoke(app, ["metric", db_id, "--remove-fields"])
    schema = api_client.api.get_form_schema(data_form_id)
    assert not any(e.code.startswith("AMOUNT_SEV") for e in schema.elements)


@pytest.fixture
def disagg_setup(api_client):
    cuid = Cuid(length=18)
    db_id = cuid.generate()
    api_client.api.add_database(
        AddDatabaseDTO(id=db_id, label="Disagg Test DB", description="Testing disagg", templateId="blank")
    )

    api_client.api.update_database(db_id, UpdateDatabaseDTO(
        resourceUpdates=[], resourceDeletions=[], languageUpdates=[], originalLanguage="en"
    ))

    folder_id = cuid.generate()
    api_client.api.update_database(db_id, UpdateDatabaseDTO(
        resourceUpdates=[Resource(id=folder_id, parentId=db_id, label="3. Data", type=DatabaseTreeResourceType.FOLDER)],
        originalLanguage="en"
    ))

    # Reference form for Ages
    ref_ages_id = cuid.generate()
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=db_id, id=ref_ages_id, label="Ref_Ages", schemaVersion=1,
            elements=[SchemaFieldDTO(id=cuid.generate(), code="REFLABEL", label="RefLabel", required=True,
                                     type=FieldType.FREE_TEXT)]
        ),
        formResource=AddFormDTO.FormResource(id=ref_ages_id, label="Ref_Ages", parentId=db_id,
                                             type=DatabaseTreeResourceType.FORM)
    ))

    # Reference form for CSL
    ref_csl_id = cuid.generate()
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=db_id, id=ref_csl_id, label="Ref_CSL", schemaVersion=1,
            elements=[SchemaFieldDTO(id=cuid.generate(), code="REFLABEL", label="RefLabel", required=True,
                                     type=FieldType.FREE_TEXT)]
        ),
        formResource=AddFormDTO.FormResource(id=ref_csl_id, label="Ref_CSL", parentId=db_id,
                                             type=DatabaseTreeResourceType.FORM)
    ))

    # Data Form
    data_form_id = cuid.generate()
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=db_id, id=data_form_id, label="Test_Data_Form", schemaVersion=1,
            elements=[
                SchemaFieldDTO(id=cuid.generate(), code="PROJ", label="Project", required=True,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="CSL", label="Caseload", required=True,
                               type=FieldType.reference,
                               typeParameters=FieldTypeParametersUpdateDTO(cardinality="single",
                                                                           range=[{"formId": ref_csl_id}])),
                SchemaFieldDTO(id=cuid.generate(), code="AMOUNT_VAL", label="Value", required=True,
                               type=FieldType.quantity,
                               typeParameters=FieldTypeParametersUpdateDTO(units="", aggregation="SUM")),
            ]
        ),
        formResource=AddFormDTO.FormResource(
            id=data_form_id, label="Test_Data_Form", parentId=folder_id, type=DatabaseTreeResourceType.FORM
        )
    ))

    # 0.3.2 Disaggregation Configuration Form
    disagg_config_id = cuid.generate()
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=db_id, id=disagg_config_id, label="0.3.2 Disaggregation Configuration", schemaVersion=1,
            elements=[
                SchemaFieldDTO(id=cuid.generate(), code="DFORM_SYSNAME", label="Form Name", required=True,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="REFORDER", label="Order", required=True,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="REFCODE", label="RefCode", required=True,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="NAME", label="Name", required=True, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="CCODE", label="C-Code", required=True,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="DFORM_EFORM_REFCODE", label="E-Form RefCode", required=True,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="OPTMAND", label="OptMand", required=True,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="RFORM_SYSNAME", label="Ref Form Name", required=True,
                               type=FieldType.FREE_TEXT),
            ]
        ),
        formResource=AddFormDTO.FormResource(
            id=disagg_config_id, label="0.3.2 Disaggregation Configuration", parentId=db_id,
            type=DatabaseTreeResourceType.FORM
        )
    ))

    return db_id, data_form_id, disagg_config_id, ref_ages_id, ref_csl_id


def test_disagg_adjustment(api_client, ai_setup, disagg_setup):
    db_id, data_form_id, disagg_config_id, ref_ages_id, ref_csl_id = disagg_setup
    base_url = ai_setup["url"]
    token = ai_setup["token"]
    os.environ["API_TOKEN"] = token
    os.environ["ACTIVITYINFO_BASE_URL"] = f"{base_url}/resources/"

    api_client.api.update_form_records([
        RecordUpdateDTO(
            formId=disagg_config_id, recordId="recdisagg",
            fields={
                "DFORM_SYSNAME": "Test_Data_Form", "REFORDER": "01", "REFCODE": "DISAG_AGE",
                "NAME": "Age", "CCODE": "DATA_AGE", "DFORM_EFORM_REFCODE": "CSL",
                "OPTMAND": "Mandatory", "RFORM_SYSNAME": "Ref_Ages"
            }
        )
    ])

    result = runner.invoke(app, ["disagg", db_id])
    assert result.exit_code == 0

    schema = api_client.api.get_form_schema(data_form_id)
    codes = [e.code for e in schema.elements]
    assert "DISAG_AGE" in codes
    csl_idx = codes.index("CSL")
    disag_idx = codes.index("DISAG_AGE")
    val_idx = codes.index("AMOUNT_VAL")
    assert csl_idx < disag_idx < val_idx


def test_disagg_remove(api_client, ai_setup, disagg_setup):
    db_id, data_form_id, disagg_config_id, ref_ages_id, ref_csl_id = disagg_setup
    base_url = ai_setup["url"]
    token = ai_setup["token"]
    os.environ["API_TOKEN"] = token
    os.environ["ACTIVITYINFO_BASE_URL"] = f"{base_url}/resources/"

    # 1. Create a disagg
    api_client.api.update_form_records([
        RecordUpdateDTO(
            formId=disagg_config_id, recordId="reccfg",
            fields={
                "DFORM_SYSNAME": "Test_Data_Form", "REFORDER": "01", "REFCODE": "DISAG_AGE",
                "NAME": "Age", "CCODE": "DATA_AGE", "DFORM_EFORM_REFCODE": "CSL",
                "OPTMAND": "Optional", "RFORM_SYSNAME": "Ref_Ages"
            }
        )
    ])
    runner.invoke(app, ["disagg", db_id])

    # Ensure schema is updated (DISAG_AGE should be there)
    schema = api_client.api.get_form_schema(data_form_id)
    assert any(e.code == "DISAG_AGE" for e in schema.elements)

    # 2. Add reference records
    api_client.api.update_form_records([
        RecordUpdateDTO(formId=ref_ages_id, recordId="ageone", fields={"REFLABEL": "0-18"}),
        RecordUpdateDTO(formId=ref_csl_id, recordId="cslone", fields={"REFLABEL": "CSL1"})
    ])

    # We must use the ID of DISAG_AGE because it was dynamically created
    disag_field = next(e for e in schema.elements if e.code == "DISAG_AGE")

    api_client.api.update_form_records([
        RecordUpdateDTO(
            formId=data_form_id, recordId="dataone",
            fields={
                "PROJ": "P1",
                "CSL": "cslone",
                disag_field.id: "ageone",
                "AMOUNT_VAL": 100
            }
        )
    ])

    # 3. Delete from config
    api_client.api.update_form_records([
        RecordUpdateDTO(formId=disagg_config_id, recordId="reccfg", deleted=True, fields={})
    ])

    # Run with remove - should delete records and then field
    result = runner.invoke(app, ["disagg", db_id, "--remove-fields"])
    assert result.exit_code == 0

    # Verify field is gone
    schema = api_client.api.get_form_schema(data_form_id)
    assert not any(e.code == "DISAG_AGE" for e in schema.elements)

    # Verify record is gone (because it had DISAG_AGE set)
    records = api_client.api.get_form(data_form_id)
    assert not any(r["@id"] == "dataone" for r in records)


@pytest.fixture
def segment_setup(api_client):
    cuid = Cuid(length=18)
    db_id = cuid.generate()
    api_client.api.add_database(
        AddDatabaseDTO(id=db_id, label="Segment Test DB", description="Testing segments", templateId="blank")
    )

    api_client.api.update_database(db_id, UpdateDatabaseDTO(
        resourceUpdates=[], resourceDeletions=[], languageUpdates=[], originalLanguage="en"
    ))

    # 1. Create 1.1 CDE Form
    cde_id = cuid.generate()
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=db_id, id=cde_id, label="1.1 Coordination Entity", schemaVersion=1,
            elements=[
                SchemaFieldDTO(id=cuid.generate(), code="NAME", label="Name", required=True, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="CDL", label="CDL", required=True, type=FieldType.reference,
                               typeParameters=FieldTypeParametersUpdateDTO(cardinality="single",
                                                                           range=[{"formId": cuid.generate()}])),
            ]
        ),
        formResource=AddFormDTO.FormResource(id=cde_id, label="1.1 Coordination Entity", parentId=db_id,
                                             type=DatabaseTreeResourceType.FORM)
    ))

    # 2. Create 1.2 LFE Form
    lfe_id = cuid.generate()
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=db_id, id=lfe_id, label="1.2 Logframe Entity", schemaVersion=1,
            elements=[
                SchemaFieldDTO(id=cuid.generate(), code="NAME", label="Name", required=True, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="CDE", label="CDE", required=True, type=FieldType.reference,
                               typeParameters=FieldTypeParametersUpdateDTO(cardinality="single",
                                                                           range=[{"formId": cde_id}])),
                SchemaFieldDTO(id=cuid.generate(), code="LFL", label="LFL", required=True, type=FieldType.reference,
                               typeParameters=FieldTypeParametersUpdateDTO(cardinality="single",
                                                                           range=[{"formId": cuid.generate()}])),
            ]
        ),
        formResource=AddFormDTO.FormResource(id=lfe_id, label="1.2 Logframe Entity", parentId=db_id,
                                             type=DatabaseTreeResourceType.FORM)
    ))

    # 3. Create 0.1.1 Entity Configuration Form
    entity_config_id = cuid.generate()
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=db_id, id=entity_config_id, label="0.1.1 Entity Configuration", schemaVersion=1,
            elements=[
                SchemaFieldDTO(id=cuid.generate(), code="SYSPREFIX", label="SysPrefix", required=True,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="REFCODE", label="RefCode", required=True,
                               type=FieldType.FREE_TEXT),
            ]
        ),
        formResource=AddFormDTO.FormResource(id=entity_config_id, label="0.1.1 Entity Configuration", parentId=db_id,
                                             type=DatabaseTreeResourceType.FORM)
    ))

    # 4. Create an Entity Form (e.g., 1.3 IND)
    ind_id = cuid.generate()
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=db_id, id=ind_id, label="1.3 Indicator", schemaVersion=1,
            elements=[
                SchemaFieldDTO(id=cuid.generate(), code="NAME", label="Name", required=True, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="ETYPE", label="EType", required=True, type=FieldType.reference,
                               typeParameters=FieldTypeParametersUpdateDTO(cardinality="single",
                                                                           range=[{"formId": cuid.generate()}])),
                SchemaFieldDTO(id=cuid.generate(), code="CDE", label="CDE", required=False, type=FieldType.reference,
                               typeParameters=FieldTypeParametersUpdateDTO(cardinality="single",
                                                                           range=[{"formId": cde_id}])),
                SchemaFieldDTO(id=cuid.generate(), code="LFE", label="LFE", required=False, type=FieldType.reference,
                               typeParameters=FieldTypeParametersUpdateDTO(cardinality="single",
                                                                           range=[{"formId": lfe_id}])),
                # Section header for Level 3 placement
                SchemaFieldDTO(id=cuid.generate(), code="ADD_DETAILS", label="Additional Details", required=False,
                               type=FieldType.section),
            ]
        ),
        formResource=AddFormDTO.FormResource(id=ind_id, label="1.3 Indicator", parentId=db_id,
                                             type=DatabaseTreeResourceType.FORM)
    ))

    # Add 1.3 to 0.1.1
    api_client.api.update_form_records([
        RecordUpdateDTO(formId=entity_config_id, recordId="recind", fields={"SYSPREFIX": "1.3", "REFCODE": "IND"})
    ])

    # 5. Create 0.1.2 Data Configuration Form
    data_config_id = cuid.generate()
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=db_id, id=data_config_id, label="0.1.2 Data Configuration", schemaVersion=1,
            elements=[
                SchemaFieldDTO(id=cuid.generate(), code="SYSNAME", label="SysName", required=True,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="CCODE", label="CCode", required=True,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="EFORM_REFCODE", label="EForm RefCode", required=True,
                               type=FieldType.FREE_TEXT),
            ]
        ),
        formResource=AddFormDTO.FormResource(id=data_config_id, label="0.1.2 Data Configuration", parentId=db_id,
                                             type=DatabaseTreeResourceType.FORM)
    ))

    # 6. Create a Data Form (e.g., Test_Data_Form)
    data_form_id = cuid.generate()
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=db_id, id=data_form_id, label="Test_Data_Form", schemaVersion=1,
            elements=[
                SchemaFieldDTO(id=cuid.generate(), code="PROJ", label="Project", required=True,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="IND", label="Indicator", required=True,
                               type=FieldType.reference,
                               typeParameters=FieldTypeParametersUpdateDTO(cardinality="single",
                                                                           range=[{"formId": ind_id}])),
            ]
        ),
        formResource=AddFormDTO.FormResource(id=data_form_id, label="Test_Data_Form", parentId=db_id,
                                             type=DatabaseTreeResourceType.FORM)
    ))

    # Add Test_Data_Form to 0.1.2
    api_client.api.update_form_records([
        RecordUpdateDTO(formId=data_config_id, recordId="recdat",
                        fields={"SYSNAME": "Test_Data_Form", "CCODE": "DAT1", "EFORM_REFCODE": "IND"})
    ])

    # 7. Create 0.3.1 Segmentation Configuration Form
    seg_config_id = cuid.generate()
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=db_id, id=seg_config_id, label="0.3.1 Segmentation Configuration", schemaVersion=1,
            elements=[
                SchemaFieldDTO(id=cuid.generate(), code="SEGDIM_REFCODE", label="SegDim RefCode", required=True,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="SEGDIM_NAME", label="SegDim Name", required=True,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="SEGDIM_TYPE", label="SegDim Type", required=True,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="SEGDIM_ORGROLE_REFCODE", label="OrgRole RefCode",
                               required=False,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="GLOBCDL_REFCODE", label="GlobCDL RefCode", required=False,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="LFL_REFCODE", label="LFL RefCode", required=False,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="SEGLEVEL_REFLEVEL", label="SegLevel RefLevel", required=True,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="OPTMAND", label="OptMand", required=True,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="SYSNAME", label="SysName", required=False,
                               type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="RFORM_NAME", label="RForm Name", required=False,
                               type=FieldType.FREE_TEXT),
            ]
        ),
        formResource=AddFormDTO.FormResource(id=seg_config_id, label="0.3.1 Segmentation Configuration", parentId=db_id,
                                             type=DatabaseTreeResourceType.FORM)
    ))

    # 8. Reference form for Segments (e.g. Modality)
    ref_mod_id = cuid.generate()
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=db_id, id=ref_mod_id, label="Operation_Modalities", schemaVersion=1,
            elements=[SchemaFieldDTO(id=cuid.generate(), code="REFLABEL", label="RefLabel", required=True,
                                     type=FieldType.FREE_TEXT)]
        ),
        formResource=AddFormDTO.FormResource(id=ref_mod_id, label="Operation_Modalities", parentId=db_id,
                                             type=DatabaseTreeResourceType.FORM)
    ))

    return {
        "db_id": db_id,
        "cde_id": cde_id,
        "lfe_id": lfe_id,
        "ind_id": ind_id,
        "data_form_id": data_form_id,
        "seg_config_id": seg_config_id,
        "ref_mod_id": ref_mod_id,
    }


def test_segment_adjustment(api_client, ai_setup, segment_setup):
    db_id = segment_setup["db_id"]
    seg_config_id = segment_setup["seg_config_id"]
    base_url = ai_setup["url"]
    token = ai_setup["token"]
    os.environ["API_TOKEN"] = token
    os.environ["ACTIVITYINFO_BASE_URL"] = f"{base_url}/resources/"

    # 1. Add a segmentation config (Modality starting at Level 1)
    api_client.api.update_form_records([
        RecordUpdateDTO(
            formId=seg_config_id, recordId="recseg1",
            fields={
                "SEGDIM_REFCODE": "SEG_MOD",
                "SEGDIM_NAME": "Modality",
                "SEGDIM_TYPE": "Reference",
                "SEGLEVEL_REFLEVEL": "1",
                "OPTMAND": "Optional",
                "SYSNAME": "Operation_Modalities",
                "RFORM_NAME": "Modality"
            }
        )
    ])

    result = runner.invoke(app, ["segment", db_id])
    assert result.exit_code == 0

    # Check Level 1 (CDE)
    schema_cde = api_client.api.get_form_schema(segment_setup["cde_id"])
    assert any(e.code == "SEG_MOD" for e in schema_cde.elements)

    # Check Level 2 (LFE) - should have inherited it as a calculated field
    schema_lfe = api_client.api.get_form_schema(segment_setup["lfe_id"])
    seg_field_lfe = next(e for e in schema_lfe.elements if e.code == "SEG_MOD")
    assert seg_field_lfe.type == FieldType.calculated
    assert "CDE.SEG_MOD" in seg_field_lfe.type_parameters.formula

    # Check Level 3 (IND) - inheritance
    schema_ind = api_client.api.get_form_schema(segment_setup["ind_id"])
    assert any(e.code == "SEG_MOD" for e in schema_ind.elements)

    # Check Level 4 (Data) - inheritance
    schema_dat = api_client.api.get_form_schema(segment_setup["data_form_id"])
    assert any(e.code == "SEG_MOD" for e in schema_dat.elements)


def test_segment_explicit_level(api_client, ai_setup, segment_setup):
    db_id = segment_setup["db_id"]
    seg_config_id = segment_setup["seg_config_id"]
    base_url = ai_setup["url"]
    token = ai_setup["token"]
    os.environ["API_TOKEN"] = token
    os.environ["ACTIVITYINFO_BASE_URL"] = f"{base_url}/resources/"

    # Modality starting at Level 1, but also explicit at Level 3
    api_client.api.update_form_records([
        RecordUpdateDTO(
            formId=seg_config_id, recordId="recseg1",
            fields={
                "SEGDIM_REFCODE": "SEG_MOD", "SEGDIM_NAME": "Modality", "SEGDIM_TYPE": "Reference",
                "SEGLEVEL_REFLEVEL": "1", "OPTMAND": "Optional", "SYSNAME": "Operation_Modalities",
                "RFORM_NAME": "Modality"
            }
        ),
        RecordUpdateDTO(
            formId=seg_config_id, recordId="recseg3",
            fields={
                "SEGDIM_REFCODE": "SEG_MOD", "SEGDIM_NAME": "Modality", "SEGDIM_TYPE": "Reference",
                "SEGLEVEL_REFLEVEL": "3", "OPTMAND": "Mandatory", "SYSNAME": "Operation_Modalities",
                "RFORM_NAME": "Modality"
            }
        )
    ])

    result = runner.invoke(app, ["segment", db_id])
    assert result.exit_code == 0

    # Check Level 3 (IND) - should have SEG_MOD_MAN (reference) and SEG_MOD (calculated with COALESCE)
    schema_ind = api_client.api.get_form_schema(segment_setup["ind_id"])
    codes = [e.code for e in schema_ind.elements]
    assert "SEG_MOD_MAN" in codes
    assert "SEG_MOD" in codes

    seg_mod = next(e for e in schema_ind.elements if e.code == "SEG_MOD")
    assert seg_mod.type == FieldType.calculated
    assert "COALESCE" in seg_mod.type_parameters.formula


def test_segment_partner(api_client, ai_setup, segment_setup):
    db_id = segment_setup["db_id"]
    seg_config_id = segment_setup["seg_config_id"]
    base_url = ai_setup["url"]
    token = ai_setup["token"]
    os.environ["API_TOKEN"] = token
    os.environ["ACTIVITYINFO_BASE_URL"] = f"{base_url}/resources/"

    # Create 2.1 form which is hardcoded for Partner type
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=db_id, id=Cuid(length=18).generate(), label="2.1 Partners", schemaVersion=1,
            elements=[SchemaFieldDTO(id=Cuid(length=18).generate(), code="REFLABEL", label="RefLabel", required=True,
                                     type=FieldType.FREE_TEXT)]
        ),
        formResource=AddFormDTO.FormResource(id=Cuid(length=18).generate(), label="2.1 Partners", parentId=db_id,
                                             type=DatabaseTreeResourceType.FORM)
    ))

    # Add Partner segmentation config
    api_client.api.update_form_records([
        RecordUpdateDTO(
            formId=seg_config_id, recordId="recsegp",
            fields={
                "SEGDIM_REFCODE": "SEG_IPORG", "SEGDIM_NAME": "Implementing Partner", "SEGDIM_TYPE": "Partner",
                "SEGDIM_ORGROLE_REFCODE": "IP",
                "SEGLEVEL_REFLEVEL": "4", "OPTMAND": "Optional"
            }
        )
    ])

    result = runner.invoke(app, ["segment", db_id])
    assert result.exit_code == 0

    schema_dat = api_client.api.get_form_schema(segment_setup["data_form_id"])
    assert any(e.code == "SEG_IPORG" for e in schema_dat.elements)

    seg_field = next(e for e in schema_dat.elements if e.code == "SEG_IPORG")
    # At level 4 it should be a reference because it's the initial level for this SEGDIM
    assert seg_field.type == FieldType.reference
    assert seg_field.key is True


def test_segment_remove(api_client, ai_setup, segment_setup):
    db_id = segment_setup["db_id"]
    seg_config_id = segment_setup["seg_config_id"]
    base_url = ai_setup["url"]
    token = ai_setup["token"]
    os.environ["API_TOKEN"] = token
    os.environ["ACTIVITYINFO_BASE_URL"] = f"{base_url}/resources/"

    # 1. Add a segmentation config and run
    api_client.api.update_form_records([
        RecordUpdateDTO(
            formId=seg_config_id, recordId="recseg1",
            fields={
                "SEGDIM_REFCODE": "SEG_MOD", "SEGDIM_NAME": "Modality", "SEGDIM_TYPE": "Reference",
                "SEGLEVEL_REFLEVEL": "1", "OPTMAND": "Optional", "SYSNAME": "Operation_Modalities",
                "RFORM_NAME": "Modality"
            }
        )
    ])
    runner.invoke(app, ["segment", db_id])
