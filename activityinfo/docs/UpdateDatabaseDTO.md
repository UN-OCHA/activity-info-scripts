# UpdateDatabaseDTO


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**resource_updates** | [**List[Resource]**](Resource.md) |  | [optional] 
**resource_deletions** | **List[str]** |  | [optional] 
**language_updates** | **List[str]** |  | [optional] 
**role_updates** | [**List[Role]**](Role.md) |  | [optional] 
**original_language** | **str** |  | [optional] 

## Example

```python
from client.models.update_database_dto import UpdateDatabaseDTO

# TODO update the JSON string below
json = "{}"
# create an instance of UpdateDatabaseDTO from a JSON string
update_database_dto_instance = UpdateDatabaseDTO.from_json(json)
# print the JSON string representation of the object
print(UpdateDatabaseDTO.to_json())

# convert the object into a dict
update_database_dto_dict = update_database_dto_instance.to_dict()
# create an instance of UpdateDatabaseDTO from a dict
update_database_dto_from_dict = UpdateDatabaseDTO.from_dict(update_database_dto_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


