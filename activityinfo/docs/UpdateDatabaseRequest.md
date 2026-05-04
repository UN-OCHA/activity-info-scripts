# UpdateDatabaseRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**resource_updates** | [**List[DatabaseResource]**](DatabaseResource.md) | List of resources to add or update. | [optional] 
**resource_deletions** | **List[str]** | List of resource ids to delete from the database. | [optional] 
**lock_updates** | [**List[DatabaseLock]**](DatabaseLock.md) | List of locks to add or update | [optional] 
**lock_deletions** | **List[str]** | List of lock ids to delete | [optional] 
**role_updates** | [**List[DatabaseRole]**](DatabaseRole.md) | List of roles to add or update | [optional] 
**role_deletions** | **List[str]** | List of role ids to delete | [optional] 
**language_updates** | **List[str]** | List of languages to add or update | [optional] 
**language_deletions** | **List[str]** | List of languages to delete | [optional] 
**original_language** | **str** | Original language update | [optional] 

## Example

```python
from client.models.update_database_request import UpdateDatabaseRequest

# TODO update the JSON string below
json = "{}"
# create an instance of UpdateDatabaseRequest from a JSON string
update_database_request_instance = UpdateDatabaseRequest.from_json(json)
# print the JSON string representation of the object
print(UpdateDatabaseRequest.to_json())

# convert the object into a dict
update_database_request_dict = update_database_request_instance.to_dict()
# create an instance of UpdateDatabaseRequest from a dict
update_database_request_from_dict = UpdateDatabaseRequest.from_dict(update_database_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


