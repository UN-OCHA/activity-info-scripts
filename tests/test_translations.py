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
    api_client.api.update_form_translations(form_id, "fr", UpdateDatabaseTranslationsDTO(strings=translations))
    
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

    # 6. Verify Target DB has the translation mapped to the new field ID
    target_translations = api_client.api.get_form_translations(target_db_id, target_form_id, "fr")
    found = any(t.translated == "Champ 1" and target_field_id in t.id for t in target_translations.translated_strings)
    assert found, f"Translation not found in target. Strings: {target_translations.translated_strings}"
