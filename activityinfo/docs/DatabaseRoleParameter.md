# DatabaseRoleParameter

Parameters defined for this role. Parameters can be referenced in filtering formulas.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**parameter_id** | **str** | This parameter&#39;s string id by which it will be referred to in formulas. | 
**label** | **str** | Human-readable label for this parameter. | 
**range** | **str** | The id of the form resource from which the parameter&#39;s values are looked up. | 

## Example

```python
from client.models.database_role_parameter import DatabaseRoleParameter

# TODO update the JSON string below
json = "{}"
# create an instance of DatabaseRoleParameter from a JSON string
database_role_parameter_instance = DatabaseRoleParameter.from_json(json)
# print the JSON string representation of the object
print(DatabaseRoleParameter.to_json())

# convert the object into a dict
database_role_parameter_dict = database_role_parameter_instance.to_dict()
# create an instance of DatabaseRoleParameter from a dict
database_role_parameter_from_dict = DatabaseRoleParameter.from_dict(database_role_parameter_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


