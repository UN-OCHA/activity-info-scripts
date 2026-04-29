# AddDatabaseUserDTO


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**email** | **str** |  | 
**name** | **str** |  | 
**locale** | **str** |  | 
**role** | [**DatabaseRole**](DatabaseRole.md) |  | 
**grants** | **List[object]** |  | [optional] 

## Example

```python
from client.models.add_database_user_dto import AddDatabaseUserDTO

# TODO update the JSON string below
json = "{}"
# create an instance of AddDatabaseUserDTO from a JSON string
add_database_user_dto_instance = AddDatabaseUserDTO.from_json(json)
# print the JSON string representation of the object
print(AddDatabaseUserDTO.to_json())

# convert the object into a dict
add_database_user_dto_dict = add_database_user_dto_instance.to_dict()
# create an instance of AddDatabaseUserDTO from a dict
add_database_user_dto_from_dict = AddDatabaseUserDTO.from_dict(add_database_user_dto_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


