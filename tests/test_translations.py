from types import SimpleNamespace

import pandas as pd
from typer.testing import CliRunner

import activityinfo_client_helpers
import translations
from activity_info_api_client.models import TranslationString
from activity_info_api_client.models.database_resource_type import DatabaseResourceType
from activity_info_api_client.models.form_schema_element_type import FormSchemaElementType
from activity_info_api_client.types import UNSET


runner = CliRunner()


def test_translation_transfer_maps_database_folder_form_and_field_ids(monkeypatch):
    source_db_id, target_db_id = "source-db", "target-db"
    source_folder_id, target_folder_id = "source-folder", "target-folder"
    source_form_id, target_form_id = "source-form", "target-form"
    source_field_id, target_field_id = "source-field", "target-field"

    def resource(resource_id, label, type_):
        return SimpleNamespace(id=resource_id, label=label, type_=type_)

    source_tree = SimpleNamespace(
        original_language="en",
        languages=["en", "fr"],
        resources=[
            resource(source_folder_id, "Reports", DatabaseResourceType.FOLDER),
            resource(source_form_id, "Monthly Data", DatabaseResourceType.FORM),
        ],
    )
    target_tree = SimpleNamespace(
        original_language="en",
        languages=["en", "fr"],
        resources=[
            resource(target_folder_id, "Reports", DatabaseResourceType.FOLDER),
            resource(target_form_id, "Monthly Data", DatabaseResourceType.FORM),
        ],
    )
    source_db_dictionary = SimpleNamespace(translated_strings=[
        TranslationString(
            id=f"resource:{source_db_id}:label", original="Source", translated="Source traduit",
            auto_translated=False,
        ),
        TranslationString(
            id=f"resource:{source_folder_id}:label", original="Reports", translated="Rapports",
            auto_translated=False,
        ),
    ])
    source_form_dictionary = SimpleNamespace(translated_strings=[
        TranslationString(
            id=f"resource:{source_form_id}:label", original="Monthly Data", translated="Monthly Data translated",
            auto_translated=False,
        ),
        TranslationString(
            id=f"field:{source_field_id}:label", original="Amount", translated="Montant",
            auto_translated=False,
        ),
    ])
    schemas = {
        source_form_id: SimpleNamespace(elements=[SimpleNamespace(id=source_field_id, label="Amount")]),
        target_form_id: SimpleNamespace(elements=[SimpleNamespace(id=target_field_id, label="Amount")]),
    }
    writes = []

    def fake_call_api(func, *args, **kwargs):
        assert kwargs["exit_on_error"] is False
        endpoint = func.__module__.rsplit(".", 1)[-1]
        if endpoint == "get_database_tree":
            result = source_tree if kwargs["database_id"] == source_db_id else target_tree
        elif endpoint == "get_database_translations":
            result = source_db_dictionary
        elif endpoint == "get_form_translations":
            result = source_form_dictionary
        elif endpoint == "get_form_schema":
            result = schemas[kwargs["form_id"]]
        else:
            writes.append((endpoint, kwargs))
            result = None
        return result, None

    monkeypatch.setattr(translations, "get_generated_client", lambda: object())
    monkeypatch.setattr(translations, "call_api", fake_call_api)

    result = runner.invoke(translations.app, ["transfer", source_db_id, target_db_id, "fr"])

    assert result.exit_code == 0, result.output
    database_write = next(item for item in writes if item[0] == "update_database_translations")
    database_strings = database_write[1]["body"].strings
    assert [(item.id, item.translated) for item in database_strings] == [
        (f"resource:{target_db_id}:label", "Source traduit"),
        (f"resource:{target_folder_id}:label", "Rapports"),
    ]

    form_write = next(item for item in writes if item[0] == "update_form_schema_translations")
    assert form_write[1]["form_id"] == target_form_id
    assert [(item.id, item.translated) for item in form_write[1]["body"].strings] == [
        (f"resource:{target_form_id}:label", "Monthly Data translated"),
        (f"field:{target_field_id}:label", "Montant"),
    ]


def test_translation_upsert_uses_generated_schema_translation_requests(monkeypatch):
    database_id = "target-db"
    folder = SimpleNamespace(id="folder", label="3_Data", type_=DatabaseResourceType.FOLDER)
    form = SimpleNamespace(id="form", label="3.1_Reference", type_=DatabaseResourceType.FORM)
    target_tree = SimpleNamespace(
        original_language="en",
        languages=["en"],
        label="Target Database",
        description=UNSET,
        roles=[],
        resources=[folder, form],
    )
    schema = SimpleNamespace(elements=[SimpleNamespace(
        id="field", code="NAME", label="Name", description=UNSET, type_parameters=UNSET,
    )])
    frames = {
        "CM - Schema": pd.DataFrame({
            "EN": ["Target Database", "3_Data", "3.1_Reference", "Name"],
            "FR": ["Base", "Données", "Références", "Nom"],
        }),
        "CM - Schema (Metrics)": pd.DataFrame({"EN": [], "FR": []}),
    }
    writes = []

    def fake_call_api(func, *args, **kwargs):
        assert kwargs["exit_on_error"] is False
        endpoint = func.__module__.rsplit(".", 1)[-1]
        if endpoint == "get_database_tree":
            return target_tree, None
        if endpoint == "get_form_schema":
            return schema, None
        writes.append((endpoint, kwargs))
        return None, None

    monkeypatch.setattr(translations, "get_generated_client", lambda: object())
    monkeypatch.setattr(translations, "call_api", fake_call_api)
    monkeypatch.setattr(translations.pd, "read_excel", lambda _path, sheet_name, **_kwargs: frames[sheet_name])

    result = runner.invoke(translations.app, [
        "upsert", "translations.xlsx", database_id, "fr", "--skip-reference", "--schema-output", "",
    ])

    assert result.exit_code == 0, result.output
    language_update = next(item for item in writes if item[0] == "update_database")
    assert language_update[1]["body"].language_updates == ["fr"]
    assert language_update[1]["body"].original_language == "en"
    database_write = next(item for item in writes if item[0] == "update_database_translations")
    assert [(item.id, item.translated) for item in database_write[1]["body"].strings] == [
        (f"resource:{database_id}:label", "Base"),
    ]
    folder_write = [item for item in writes if item[0] == "update_database_translations"][1]
    assert [(item.id, item.translated) for item in folder_write[1]["body"].strings] == [
        ("resource:folder:label", "Données"),
    ]
    form_write = next(item for item in writes if item[0] == "update_form_schema_translations")
    assert [(item.id, item.translated) for item in form_write[1]["body"].strings] == [
        ("resource:form:label", "Références"),
        ("field:field:label", "Nom"),
    ]


def test_translation_upsert_updates_reference_records_with_generated_models(monkeypatch):
    database_id = "target-db"
    form = SimpleNamespace(id="form", label="3.1_Reference", type_=DatabaseResourceType.FORM)
    target_tree = SimpleNamespace(
        original_language="en", languages=["en", "fr"], label="Target Database", description=UNSET,
        roles=[], resources=[form],
    )
    schema = SimpleNamespace(elements=[SimpleNamespace(
        id="field-id", code="NAME_FR", type_=FormSchemaElementType.FREE_TEXT,
    )])
    records = [SimpleNamespace(to_dict=lambda: {
        "@id": "record-1", "REFCODE": "R1", "NAME_FR": "", "CCODE": "R1",
    })]
    updates = []
    values_frame = pd.DataFrame({
        "Form System Prefix": ["3.1"], "Field Code": ["NAME"], "Refcode": ["R1"], "FR": ["Nom traduit"],
    })

    def fake_call_api(func, *args, **kwargs):
        assert kwargs["exit_on_error"] is False
        endpoint = func.__module__.rsplit(".", 1)[-1]
        if endpoint == "get_database_tree":
            return target_tree, None
        if endpoint == "get_form_schema":
            return schema, None
        if endpoint == "get_form_records":
            return records, None
        updates.append(kwargs["body"])
        return None, None

    monkeypatch.setattr(translations, "get_generated_client", lambda: object())
    monkeypatch.setattr(translations, "call_api", fake_call_api)
    monkeypatch.setattr(translations.pd, "read_excel", lambda *_args, **_kwargs: values_frame)

    result = runner.invoke(translations.app, ["upsert", "translations.xlsx", database_id, "fr", "--skip-schema"])

    assert result.exit_code == 0, result.output
    assert len(updates) == 1
    assert updates[0].to_dict() == {
        "changes": [{
            "formId": "form", "recordId": "record-1", "fields": {"field-id": "Nom traduit"}, "deleted": False,
        }],
    }


def test_generated_record_helper_resolves_multiselect_references():
    base_records = [SimpleNamespace(to_dict=lambda: {"@id": "record-1", "REFS.@id": "ref-1, ref-2"})]
    reference_records = [
        SimpleNamespace(to_dict=lambda: {"@id": "ref-1", "LABEL": "First"}),
        SimpleNamespace(to_dict=lambda: {"@id": "ref-2", "LABEL": "Second"}),
    ]
    schema = SimpleNamespace(elements=[SimpleNamespace(
        type_=FormSchemaElementType.MULTISELECTREFERENCE,
        code="REFS",
        type_parameters=SimpleNamespace(range_=[SimpleNamespace(form_id="reference-form")]),
    )])
    requested_forms = []

    def fake_request(_client, endpoint, **kwargs):
        endpoint_name = endpoint.__name__.rsplit(".", 1)[-1]
        if endpoint_name == "get_form_schema":
            return schema
        requested_forms.append(kwargs["form_id"])
        return reference_records if kwargs["form_id"] == "reference-form" else base_records

    records = activityinfo_client_helpers.get_form_records_with_multiref(
        object(), "base-form", request_api=fake_request
    )

    assert requested_forms == ["base-form", "reference-form"]
    assert [item["LABEL"] for item in records[0]["REFS"]] == ["First", "Second"]
