"""Higher-level helpers for working with the generated ActivityInfo client."""

from collections.abc import Callable, Sequence
from typing import Any

from activity_info_api_client.api.default import get_form_records, get_form_schema
from activity_info_api_client.client import Client
from activity_info_api_client.models import DatabaseResource, FormSchemaElementType
from activity_info_api_client.types import UNSET


def find_resource_by_prefix(
    items: Sequence[DatabaseResource], prefix: str
) -> DatabaseResource | None:
    """Find a resource by prefix, preferring exact and standard-delimiter matches."""
    matches = [item for item in items if item.label.startswith(prefix)]
    if not matches:
        return None

    for item in matches:
        if item.label == prefix:
            return item

    for item in matches:
        if len(item.label) > len(prefix) and item.label[len(prefix)] in (".", "_"):
            return item

    return matches[0]


def get_form_records_with_multiref(
    client: Client,
    form_id: str,
    *,
    request_api: Callable[..., Any],
) -> list[dict[str, Any]]:
    """Fetch a form's records and resolve multiselect reference IDs to records."""
    records = [
        record.to_dict()
        for record in request_api(client, get_form_records, form_id=form_id)
    ]
    schema = request_api(client, get_form_schema, form_id=form_id)

    for field in schema.elements:
        if field.type_ != FormSchemaElementType.MULTISELECTREFERENCE or field.code is UNSET:
            continue

        parameters = field.type_parameters
        if parameters is UNSET or parameters.range_ is UNSET or not parameters.range_:
            continue

        referenced_form_id = parameters.range_[0].form_id
        if referenced_form_id is UNSET:
            continue

        referenced_records = [
            record.to_dict()
            for record in request_api(client, get_form_records, form_id=referenced_form_id)
        ]
        referenced_records_by_id = {record["@id"]: record for record in referenced_records}
        field_id_key = f"{field.code}.@id"

        for record in records:
            ids_value = record.get(field_id_key)
            if ids_value:
                ids = [value.strip() for value in ids_value.split(",")]
                record[field.code] = [
                    referenced_records_by_id[value]
                    for value in ids
                    if value in referenced_records_by_id
                ]
            else:
                record[field.code] = []

    return records
