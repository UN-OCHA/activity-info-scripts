# DatabaseRole


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** |  | 
**parameters** | **Dict[str, object]** |  | [optional] 
**resources** | **List[object]** |  | [optional] [default to []]

## Example

```python
from client.models.database_role import DatabaseRole

# TODO update the JSON string below
json = "{}"
# create an instance of DatabaseRole from a JSON string
database_role_instance = DatabaseRole.from_json(json)
# print the JSON string representation of the object
print(DatabaseRole.to_json())

# convert the object into a dict
database_role_dict = database_role_instance.to_dict()
# create an instance of DatabaseRole from a dict
database_role_from_dict = DatabaseRole.from_dict(database_role_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


