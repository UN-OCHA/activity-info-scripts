# FormPermissions


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**view** | **bool** |  | 
**view_filter** | **str** |  | [optional] 
**create_record** | **bool** |  | 
**create_filter** | **str** |  | [optional] 
**edit_record** | **bool** |  | 
**edit_filter** | **str** |  | [optional] 
**delete_record** | **bool** |  | 
**delete_filter** | **str** |  | [optional] 
**bulk_delete** | **bool** |  | [optional] 
**export_records** | **bool** |  | [optional] 
**resolve_duplicates** | **bool** |  | [optional] 
**manage_import_configs** | **bool** |  | [optional] 
**update_schema** | **bool** |  | [optional] 
**collection_links** | **bool** |  | [optional] 
**public_form** | **bool** |  | [optional] 
**api** | **bool** |  | [optional] 
**security_categories** | **List[str]** |  | [optional] 
**locks** | [**List[DatabaseLock]**](DatabaseLock.md) |  | [optional] 
**field_level_conditions** | [**List[FieldCondition]**](FieldCondition.md) |  | [optional] 

## Example

```python
from client.models.form_permissions import FormPermissions

# TODO update the JSON string below
json = "{}"
# create an instance of FormPermissions from a JSON string
form_permissions_instance = FormPermissions.from_json(json)
# print the JSON string representation of the object
print(FormPermissions.to_json())

# convert the object into a dict
form_permissions_dict = form_permissions_instance.to_dict()
# create an instance of FormPermissions from a dict
form_permissions_from_dict = FormPermissions.from_dict(form_permissions_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


