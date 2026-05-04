# DatabaseRole


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | This role&#39;s id | 
**label** | **str** | This role&#39;s human-readable label | 
**permissions** | [**List[Permission]**](Permission.md) | Permissions granted this role. Applicable for legacy roles, for grant based roles it will be limited to permissions that are NOT related to resources, like MANAGE_USERS, MANAGE_ROLES | [optional] 
**parameters** | [**List[DatabaseRoleParameter]**](DatabaseRoleParameter.md) | Parameters defined for this role. Parameters can be referenced in filtering formulas. | [optional] 
**filters** | [**List[DatabaseRoleFilter]**](DatabaseRoleFilter.md) | Pre-defined filters. Role filters allow other users to choose filters for permissions without having to write formulas themselves. -- NOTEWORTHY - only used by legacy roles | [optional] 
**grants** | [**List[Grant]**](Grant.md) |  | [optional] 
**version** | **int** |  | [optional] [default to 0]
**grant_based** | **bool** |  | [optional] [default to True]

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


