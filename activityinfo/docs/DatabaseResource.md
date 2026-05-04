# DatabaseResource

A resource (folder, form or subform) that belongs to a database

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | The resource&#39;s id. Form and subform resources must have globally-unique ids (across all of ActivityInfo) but folder ids are only guaranteed to be unique within a single database. | 
**type** | **str** | The resource type. Values: DATABASE, FOLDER, REPORT, FORM, SUB_FORM | [optional] 
**parent_id** | **str** | The id of this resource&#39;s parent, for example, a folder id. If this is a top-level folder or form, then the parentId will be the id of the database itself. | [optional] 
**label** | **str** | The resource&#39;s human-readable label. | [optional] 
**visibility** | **str** | The resource&#39;s visibility. Values: PUBLIC, PRIVATE, REFERENCE | [optional] 
**icon** | **str** | Icon for the resource. | [optional] 

## Example

```python
from client.models.database_resource import DatabaseResource

# TODO update the JSON string below
json = "{}"
# create an instance of DatabaseResource from a JSON string
database_resource_instance = DatabaseResource.from_json(json)
# print the JSON string representation of the object
print(DatabaseResource.to_json())

# convert the object into a dict
database_resource_dict = database_resource_instance.to_dict()
# create an instance of DatabaseResource from a dict
database_resource_from_dict = DatabaseResource.from_dict(database_resource_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


