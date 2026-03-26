from cuid2 import Cuid
from typer.testing import CliRunner

from api.models import AddDatabaseDTO, AddFormDTO, DatabaseTreeResourceType, DatabaseTreeResourceVisibility, \
    SchemaFieldDTO, FieldType, UpdateDatabaseTranslationsDTO, DatabaseTranslation
from translations import app

runner = CliRunner()


def test_translation_transfer(api_client, ai_setup):
    base_url = ai_setup["url"]
    token = ai_setup["token"]

    cuid = Cuid(length=18)
    source_db_id = cuid.generate()
    target_db_id = cuid.generate()

    # 1. Create Source and Target DBs
    api_client.api.add_database(
        AddDatabaseDTO(id=source_db_id, label="Source DB", description="Source", templateId="blank"))
    api_client.api.add_database(
        AddDatabaseDTO(id=target_db_id, label="Target DB", description="Target", templateId="blank"))

    # Add 'fr' language to both
    from api.models import UpdateDatabaseDTO
    api_client.api.update_database(source_db_id, UpdateDatabaseDTO(
        languageUpdates=["fr"], resourceUpdates=[], resourceDeletions=[], originalLanguage="en"))
    api_client.api.update_database(target_db_id, UpdateDatabaseDTO(
        languageUpdates=["fr"], resourceUpdates=[], resourceDeletions=[], originalLanguage="en"))

    # 2. Add a form to Source DB with some fields
    form_id = cuid.generate()
    field_id = cuid.generate()
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=source_db_id,
            id=form_id,
            label="Test Form",
            schemaVersion=1,
            elements=[
                SchemaFieldDTO(id=field_id, code="F1", label="Field 1", required=True, type=FieldType.FREE_TEXT)
            ]
        ),
        formResource=AddFormDTO.FormResource(
            id=form_id,
            label="Test Form",
            parentId=source_db_id,
            type=DatabaseTreeResourceType.FORM,
            visibility=DatabaseTreeResourceVisibility.PRIVATE
        )
    ))

    # 3. Add same form to Target DB (simulating a clone or migration)
    target_form_id = cuid.generate()
    target_field_id = cuid.generate()
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=target_db_id,
            id=target_form_id,
            label="Test Form",
            schemaVersion=1,
            elements=[
                SchemaFieldDTO(id=target_field_id, code="F1", label="Field 1", required=True, type=FieldType.FREE_TEXT)
            ]
        ),
        formResource=AddFormDTO.FormResource(
            id=target_form_id,
            label="Test Form",
            parentId=target_db_id,
            type=DatabaseTreeResourceType.FORM,
            visibility=DatabaseTreeResourceVisibility.PRIVATE
        )
    ))

    # 4. Seed Source translations for 'fr'
    translations = [
        DatabaseTranslation(id=f"field:{field_id}:label", original="Field 1", translated="Champ 1", autoTranslated=False)
    ]
    api_client.api.update_form_translations(target_db_id, form_id, "fr", UpdateDatabaseTranslationsDTO(strings=translations))
    
    # Verify seeding
    source_translations = api_client.api.get_form_translations(source_db_id, form_id, "fr")
    print(f"DEBUG: Source translations after seeding: {source_translations.translated_strings}")

    # 5. Run the transfer command
    import os
    os.environ["API_TOKEN"] = token
    os.environ["ACTIVITYINFO_BASE_URL"] = f"{base_url}/resources/"

    target_tree = api_client.api.get_database_tree(target_db_id)
    print(f"DEBUG: Target tree resources: {[ (r.label, r.type) for r in target_tree.resources]}")

    print(f"DEBUG: source_db_id='{source_db_id}', target_db_id='{target_db_id}', language='fr'")
    result = runner.invoke(app, [source_db_id, target_db_id, "fr"])
    
    print("--- CLI OUTPUT START ---")
    print(result.output)
    print("--- CLI OUTPUT END ---")
    
    assert result.exit_code == 0

def test_translation_transfer_database_and_folders(api_client, ai_setup):
    base_url = ai_setup["url"]
    token = ai_setup["token"]

    cuid = Cuid(length=18)
    source_db_id = cuid.generate()
    target_db_id = cuid.generate()

    # 1. Create Source and Target DBs
    api_client.api.add_database(
        AddDatabaseDTO(id=source_db_id, label="Source DB", description="Source", templateId="blank"))
    api_client.api.add_database(
        AddDatabaseDTO(id=target_db_id, label="Target DB", description="Target", templateId="blank"))

    from api.models import UpdateDatabaseDTO
    api_client.api.update_database(source_db_id, UpdateDatabaseDTO(
        languageUpdates=["fr"], resourceUpdates=[], resourceDeletions=[], originalLanguage="en"))
    api_client.api.update_database(target_db_id, UpdateDatabaseDTO(
        languageUpdates=["fr"], resourceUpdates=[], resourceDeletions=[], originalLanguage="en"))

    # 2. Add Folder and Form to Source
    folder_id = cuid.generate()
    form_id = cuid.generate()
    field_id = cuid.generate()

    # Add Folder
    from api.models import Resource
    api_client.api.update_database(source_db_id, UpdateDatabaseDTO(
        resourceUpdates=[
            Resource(id=folder_id, parentId=source_db_id, label="Test Folder", type=DatabaseTreeResourceType.FOLDER)
        ]
    ))
    
    # Add Form
    from api.models import AddFormDTO
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=source_db_id, id=form_id, label="Nested Form", schemaVersion=1,
            elements=[SchemaFieldDTO(id=field_id, code="F1", label="Field 1", required=True, type=FieldType.FREE_TEXT)]
        ),
        formResource=AddFormDTO.FormResource(id=form_id, label="Nested Form", parentId=folder_id, type=DatabaseTreeResourceType.FORM)
    ))

    # 3. Add Folder and Form to Target (different IDs)
    target_folder_id = cuid.generate()
    target_form_id = cuid.generate()
    target_field_id = cuid.generate()

    # Add Folder
    api_client.api.update_database(target_db_id, UpdateDatabaseDTO(
        resourceUpdates=[
            Resource(id=target_folder_id, parentId=target_db_id, label="Test Folder", type=DatabaseTreeResourceType.FOLDER)
        ]
    ))
    
    # Add Form
    api_client.api.add_form(AddFormDTO(
        formClass=AddFormDTO.FormClass(
            databaseId=target_db_id, id=target_form_id, label="Nested Form", schemaVersion=1,
            elements=[SchemaFieldDTO(id=target_field_id, code="F1", label="Field 1", required=True, type=FieldType.FREE_TEXT)]
        ),
        formResource=AddFormDTO.FormResource(id=target_form_id, label="Nested Form", parentId=target_folder_id, type=DatabaseTreeResourceType.FORM)
    ))

    # 4. Seed Database-Level Translations
    db_translations = [
        DatabaseTranslation(id=f"resource:{source_db_id}:label", original="Source DB", translated="BD Source", autoTranslated=False),
        DatabaseTranslation(id=f"resource:{folder_id}:label", original="Test Folder", translated="Dossier Test", autoTranslated=False)
    ]
    api_client.api.update_database_translations(source_db_id, "fr", UpdateDatabaseTranslationsDTO(strings=db_translations))

    # 5. Seed Form-Level Translations
    form_translations = [
        DatabaseTranslation(id=f"resource:{form_id}:label", original="Nested Form", translated="Formulaire Imbriqué", autoTranslated=False),
        DatabaseTranslation(id=f"field:{field_id}:label", original="Field 1", translated="Champ 1", autoTranslated=False)
    ]
    api_client.api.update_form_translations(target_db_id, form_id, "fr", UpdateDatabaseTranslationsDTO(strings=form_translations))

    # 6. Run the transfer command
    import os
    os.environ["API_TOKEN"] = token
    os.environ["ACTIVITYINFO_BASE_URL"] = f"{base_url}/resources/"

    result = runner.invoke(app, [source_db_id, target_db_id, "fr"])
    assert result.exit_code == 0

    # 7. Verify Results
    # 7.1 Database Level
    target_db_trans = api_client.api.get_database_translations(target_db_id, "fr")
    # Verify Folder label mapped (This is the key fix for folder IDs)
    assert any(t.translated == "Dossier Test" and target_folder_id in t.id for t in target_db_trans.translated_strings)

    # 7.2 Form Level
    target_form_trans = api_client.api.get_form_translations(target_db_id, target_form_id, "fr")
    # Verify Form label mapped
    assert any(t.translated == "Formulaire Imbriqué" and target_form_id in t.id for t in target_form_trans.translated_strings)
    # Verify Field label mapped
    assert any(t.translated == "Champ 1" and target_field_id in t.id for t in target_form_trans.translated_strings)
