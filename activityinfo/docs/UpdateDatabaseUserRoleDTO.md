# UpdateDatabaseUserRoleDTO


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**assignments** | [**List[DatabaseRole]**](DatabaseRole.md) |  | 

## Example

```python
from client.models.update_database_user_role_dto import UpdateDatabaseUserRoleDTO

# TODO update the JSON string below
json = "{}"
# create an instance of UpdateDatabaseUserRoleDTO from a JSON string
update_database_user_role_dto_instance = UpdateDatabaseUserRoleDTO.from_json(json)
# print the JSON string representation of the object
print(UpdateDatabaseUserRoleDTO.to_json())

# convert the object into a dict
update_database_user_role_dto_dict = update_database_user_role_dto_instance.to_dict()
# create an instance of UpdateDatabaseUserRoleDTO from a dict
update_database_user_role_dto_from_dict = UpdateDatabaseUserRoleDTO.from_dict(update_database_user_role_dto_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


