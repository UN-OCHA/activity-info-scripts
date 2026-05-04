# DatabaseRoleFilter

Pre-defined filters. Role filters allow other users to choose filters for permissions without having to write formulas themselves. -- NOTEWORTHY - only used by legacy roles.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | This filter&#39;s id. | 
**label** | **str** | This filter&#39;s human-readable label. | 
**filter** | **str** | A formula that can be used to filter a record-level permission. | 

## Example

```python
from client.models.database_role_filter import DatabaseRoleFilter

# TODO update the JSON string below
json = "{}"
# create an instance of DatabaseRoleFilter from a JSON string
database_role_filter_instance = DatabaseRoleFilter.from_json(json)
# print the JSON string representation of the object
print(DatabaseRoleFilter.to_json())

# convert the object into a dict
database_role_filter_dict = database_role_filter_instance.to_dict()
# create an instance of DatabaseRoleFilter from a dict
database_role_filter_from_dict = DatabaseRoleFilter.from_dict(database_role_filter_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


