from typing import List

from api import ActivityInfoClient
from api.models import DatabaseTree, DatabaseTreeResourceType, Resource, FieldType, FormSchema

# Folders prefixed with these numbers are considered 'Data' folders in our standard structure
DATA_FOLDER_PREFIXES = ["3", "4", "5", "6"]


def filter_data_forms(tree: DatabaseTree, folder_id: str) -> List[Resource]:
    """
    Filter the database tree to find only 'Data Forms'.
    
    Data forms are identified as being children of folders whose labels start with 
    the recognized data folder prefixes (3, 4, 5, or 6).
    
    Args:
        tree: The full database tree resource.
        folder_id: The ID of the parent folder or database to search within.
        
    Returns:
        A list of Resource objects representing the discovered data forms.
    """
    # 1. Identify valid parent folders
    top_level_folders = [
        res for res in tree.resources
        if res.type == DatabaseTreeResourceType.FOLDER
           and res.parentId == folder_id
           and res.label.startswith(tuple(DATA_FOLDER_PREFIXES))
    ]

    # 2. Return forms that reside within those folders
    return [
        res for res in tree.resources
        if res.type == DatabaseTreeResourceType.FORM
           and res.parentId in [folder.id for folder in top_level_folders]
    ]


def get_records_with_multiref(client: ActivityInfoClient, form_id: str):
    """
    Fetch records for a form and resolve multi-select reference fields.
    
    By default, the ActivityInfo API returns multi-select reference values as a 
    comma-separated string of IDs. This helper fetches the target records and 
    replaces those IDs with the actual record dictionaries for easier processing.
    
    Args:
        client: An authenticated ActivityInfoClient.
        form_id: The ID of the form to fetch records from.
        
    Returns:
        A list of record dictionaries with resolved multi-select references.
    """
    base_records = client.api.get_form(form_id)
    schema = client.api.get_form_schema(form_id)

    # Identify fields that use multi-select references
    multiref_fields = [
        field for field in schema.elements
        if field.type == FieldType.multiselectreference
    ]

    for field in multiref_fields:
        # Skip fields without a defined target range (should not happen in valid schemas)
        if not field.type_parameters or not field.type_parameters.range:
            continue

        # Fetch the records of the referenced form to build a lookup map
        ref_form_id = field.type_parameters.range[0]["formId"]
        ref_records = client.api.get_form(ref_form_id)
        ref_records_map = {rec["@id"]: rec for rec in ref_records}

        # The ID key is usually 'CODE.@id' in the API response
        field_id_key = f"{field.code}.@id"

        for record in base_records:
            # Parse the comma-separated string of IDs
            ids_str = record.get(field_id_key)
            if ids_str:
                ids = [i.strip() for i in ids_str.split(",")]
                # Replace the field value with a list of fully resolved record objects
                record[field.code] = [
                    ref_records_map[i] for i in ids if i in ref_records_map
                ]
            else:
                # Ensure an empty list if no references are selected
                record[field.code] = []

    return base_records


def get_field_info(schema: FormSchema):
    """
    Determine the primary label field for a form based on standard precedence rules.
    
    Precedence:
    1. Explicitly defined 'recordLabelFieldId' in the schema.
    2. A field with the code 'REFLABEL'.
    3. A field with the code 'NAME'.
    4. The first element in the schema list.
    
    Args:
        schema: The FormSchema object to analyze.
        
    Returns:
        A tuple of (field_id, field_label) or (None, None) if no fields are found.
    """
    # 1. Check explicit label field ID
    if schema.record_label_field_id:
        field = next((f for f in schema.elements if f.id == schema.record_label_field_id), None)
        if field:
            return field.id, field.label

    # 2. Fallback to standard codes
    for code in ["REFLABEL", "NAME"]:
        field = next((f for f in schema.elements if f.code == code), None)
        if field:
            return field.id, field.label

    # 3. Final fallback to the very first field defined
    if schema.elements:
        return schema.elements[0].id, schema.elements[0].label
    
    return None, None
