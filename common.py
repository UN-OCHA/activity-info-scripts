from typing import List, Optional, Dict, Any, cast

from activityinfo.client import DatabaseResource, FormSchema, DefaultApi, Database
from activityinfo.client.models.range_inner import RangeInner


def require_label(label: Optional[str]) -> str:
    if not label:
        raise ValueError("Resource label is required")
    return label


def label_starts_with(resource: DatabaseResource, prefix: str) -> bool:
    return resource.label is not None and resource.label.startswith(prefix)

# Folders prefixed with these numbers are considered 'Data' folders in our standard structure
DATA_FOLDER_PREFIXES = ["3", "4", "5", "6"]


def filter_data_forms(tree: Database, folder_id: str) -> List[DatabaseResource]:
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
        if res.type == "FOLDER"
           and res.parent_id == folder_id
           and res.label is not None
           and res.label.startswith(tuple(DATA_FOLDER_PREFIXES))
    ]

    # 2. Return forms that reside within those folders
    return [
        res for res in tree.resources
        if res.type == "FORM"
           and res.parent_id in [folder.id for folder in top_level_folders]
    ]


def find_resource_by_prefix(items: List[DatabaseResource], prefix: str) -> Optional[DatabaseResource]:
    """
    Find an item by its 'label' attribute prefix, preferring matches followed by standard delimiters.
    
    Priority:
    1. Exact match (label == prefix)
    2. Match followed by '.' or '_' (e.g., prefix='3.1' matches '3.1.' or '3.1_')
    3. Any other match starting with the prefix (e.g., prefix='3.1' matches '3.1A')
    
    Args:
        items: List of objects (must have a 'label' attribute).
        prefix: The label prefix to look for.
        
    Returns:
        The best matching item or None.
    """
    matches = find_all_resources_by_prefix(items, prefix)
    if not matches:
        return None

    # 1. Exact match
    for item in matches:
        if require_label(item.label) == prefix:
            return item

    # 2. Match followed by '.' or '_'
    for item in matches:
        item_label = require_label(item.label)
        if len(item_label) > len(prefix) and item_label[len(prefix)] in (".", "_"):
            return item

    # 3. Fallback to the first match (e.g., prefix='3.1' matches '3.1A')
    return matches[0]


def find_all_resources_by_prefix(items: List[DatabaseResource], prefix: str) -> List[DatabaseResource]:
    """
    Find all items whose 'label' attribute starts with the given prefix.
    
    Args:
        items: List of objects (must have a 'label' attribute).
        prefix: The label prefix to look for.
        
    Returns:
        A list of matching items.
    """
    return [item for item in items if item.label is not None and item.label.startswith(prefix)]


async def get_records_with_multiref(client: DefaultApi, form_id: str) -> List[Dict[str, Any]]:
    """
    Fetch records for a form and resolve multi-select reference fields.
    
    By default, the ActivityInfo API returns multi-select reference values as a 
    comma-separated string of IDs. This helper fetches the target records and 
    replaces those IDs with the actual record dictionaries for easier processing.
    
    Args:
        client: An authenticated DefaultApi.
        form_id: The ID of the form to fetch records from.
        
    Returns:
        A list of record dictionaries with resolved multi-select references.
    """
    base_records = cast(List[Dict[str, Any]], await client.get_form_records(form_id=form_id))
    schema = await client.get_form_schema(form_id=form_id)

    # Identify fields that use multi-select references
    multiref_fields = [
        field for field in schema.elements
        if field.type == "reference"
    ]

    for field in multiref_fields:
        # Skip fields without a code or defined target range
        if not field.code or not field.type_parameters or not field.type_parameters.range:
            continue

        field_code = field.code

        # Fetch the records of the referenced form to build a lookup map
        first_range = field.type_parameters.range[0]
        ref_form_id = cast(
            Optional[str],
            first_range.form_id if isinstance(first_range, RangeInner) else first_range.get("formId"),
        )
        if not ref_form_id:
            continue
        ref_records = cast(List[Dict[str, Any]], await client.get_form_records(form_id=ref_form_id))
        ref_records_map = {rec["@id"]: rec for rec in ref_records}

        # The ID key is usually 'CODE.@id' in the API response
        field_id_key = f"{field_code}.@id"

        for record in base_records:
            # Parse the comma-separated string of IDs
            ids_str = record.get(field_id_key)
            if ids_str:
                ids = [i.strip() for i in ids_str.split(",")]
                # Replace the field value with a list of fully resolved record objects
                record[field_code] = [
                    ref_records_map[i] for i in ids if i in ref_records_map
                ]
            else:
                # Ensure an empty list if no references are selected
                record[field_code] = []

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
