import os
import pytest
from typer.testing import CliRunner
from api.models import (
    AddDatabaseDTO, AddFormDTO, DatabaseTreeResourceType,
    DatabaseTreeResourceVisibility, SchemaFieldDTO, FieldType,
    RecordUpdateDTO, UpdateDatabaseDTO, Resource, FieldTypeParametersUpdateDTO,
    TypeParameterLookupConfig
)
from config import app
from cuid2 import Cuid

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
            elements=[SchemaFieldDTO(id=cuid.generate(), code="REFLABEL", label="Label", required=True, type=FieldType.FREE_TEXT)]
        ),
        formResource=AddFormDTO.FormResource(id=ref_id, label="Ref_CSL", parentId=db_id, type=DatabaseTreeResourceType.FORM)
    ))

    # Create a Data Form
    data_form_id = cuid.generate()
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=db_id, id=data_form_id, label="Test_Data_Form", schemaVersion=1,
            elements=[
                SchemaFieldDTO(id=cuid.generate(), code="PROJ", label="Project", required=True, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="CSL", label="Caseload", required=True, type=FieldType.reference,
                               type_parameters=FieldTypeParametersUpdateDTO(cardinality="single", range=[{"formId": ref_id}])),
                SchemaFieldDTO(id=cuid.generate(), code="OTHER", label="Other Field", required=False, type=FieldType.FREE_TEXT),
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
                SchemaFieldDTO(id=cuid.generate(), code="DFORM_SYSNAME", label="Form Name", required=True, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="REFORDER", label="Order", required=True, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="DISPLAY_REFCODE", label="Display", required=True, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="NAME", label="Name", required=True, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="REFCODE_MAN", label="RefCode Man", required=False, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="REFCODE", label="RefCode", required=True, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="CCODE", label="C-Code", required=True, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="EFORM_REFCODE", label="E-Form RefCode", required=True, type=FieldType.FREE_TEXT),
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
    api_client.api.update_form_records([RecordUpdateDTO(formId=metric_config_id, recordId="recmr", deleted=True, fields={})])
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
            elements=[SchemaFieldDTO(id=cuid.generate(), code="REFLABEL", label="RefLabel", required=True, type=FieldType.FREE_TEXT)]
        ),
        formResource=AddFormDTO.FormResource(id=ref_ages_id, label="Ref_Ages", parentId=db_id, type=DatabaseTreeResourceType.FORM)
    ))

    # Reference form for CSL
    ref_csl_id = cuid.generate()
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=db_id, id=ref_csl_id, label="Ref_CSL", schemaVersion=1,
            elements=[SchemaFieldDTO(id=cuid.generate(), code="REFLABEL", label="RefLabel", required=True, type=FieldType.FREE_TEXT)]
        ),
        formResource=AddFormDTO.FormResource(id=ref_csl_id, label="Ref_CSL", parentId=db_id, type=DatabaseTreeResourceType.FORM)
    ))

    # Data Form
    data_form_id = cuid.generate()
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=db_id, id=data_form_id, label="Test_Data_Form", schemaVersion=1,
            elements=[
                SchemaFieldDTO(id=cuid.generate(), code="PROJ", label="Project", required=True, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="CSL", label="Caseload", required=True, type=FieldType.reference,
                               type_parameters=FieldTypeParametersUpdateDTO(cardinality="single", range=[{"formId": ref_csl_id}])),
                SchemaFieldDTO(id=cuid.generate(), code="AMOUNT_VAL", label="Value", required=True, type=FieldType.quantity,
                               type_parameters=FieldTypeParametersUpdateDTO(units="", aggregation="SUM")),
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
                SchemaFieldDTO(id=cuid.generate(), code="DFORM_SYSNAME", label="Form Name", required=True, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="REFORDER", label="Order", required=True, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="REFCODE", label="RefCode", required=True, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="NAME", label="Name", required=True, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="CCODE", label="C-Code", required=True, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="DFORM_EFORM_REFCODE", label="E-Form RefCode", required=True, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="OPTMAND", label="OptMand", required=True, type=FieldType.FREE_TEXT),
                SchemaFieldDTO(id=cuid.generate(), code="RFORM_SYSNAME", label="Ref Form Name", required=True, type=FieldType.FREE_TEXT),
            ]
        ),
        formResource=AddFormDTO.FormResource(
            id=disagg_config_id, label="0.3.2 Disaggregation Configuration", parentId=db_id, type=DatabaseTreeResourceType.FORM
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
