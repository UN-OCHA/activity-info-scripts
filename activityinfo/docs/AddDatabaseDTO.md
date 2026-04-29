# AddDatabaseDTO


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** |  | 
**label** | **str** |  | 
**description** | **str** |  | 
**template_id** | **str** |  | 

## Example

```python
from client.models.add_database_dto import AddDatabaseDTO

# TODO update the JSON string below
json = "{}"
# create an instance of AddDatabaseDTO from a JSON string
add_database_dto_instance = AddDatabaseDTO.from_json(json)
# print the JSON string representation of the object
print(AddDatabaseDTO.to_json())

# convert the object into a dict
add_database_dto_dict = add_database_dto_instance.to_dict()
# create an instance of AddDatabaseDTO from a dict
add_database_dto_from_dict = AddDatabaseDTO.from_dict(add_database_dto_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


