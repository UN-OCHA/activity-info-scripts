from typing import List

from api import ActivityInfoClient
from api.models import DatabaseTree, DatabaseTreeResourceType, Resource, FieldType, FormSchema

DATA_FOLDER_PREFIXES = ["3", "4", "5", "6"]


def filter_data_forms(tree: DatabaseTree, folder_id: str) -> List[Resource]:
    top_level_folders = [
        res for res in tree.resources
        if res.type == DatabaseTreeResourceType.FOLDER
           and res.parentId == folder_id
           and res.label.startswith(tuple(DATA_FOLDER_PREFIXES))
    ]

    return [
        res for res in tree.resources
        if res.type == DatabaseTreeResourceType.FORM
           and res.parentId in [folder.id for folder in top_level_folders]
    ]


def get_records_with_multiref(client: ActivityInfoClient, form_id: str):
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
