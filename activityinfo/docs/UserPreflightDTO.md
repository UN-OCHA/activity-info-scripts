# UserPreflightDTO


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**email** | **str** |  | 
**name** | **str** |  | [optional] [default to '']
**locale** | **str** |  | [optional] [default to '']
**role** | [**DatabaseRole**](DatabaseRole.md) |  | [optional] 
**grants** | **List[object]** |  | [optional] 

## Example

```python
from client.models.user_preflight_dto import UserPreflightDTO

# TODO update the JSON string below
json = "{}"
# create an instance of UserPreflightDTO from a JSON string
user_preflight_dto_instance = UserPreflightDTO.from_json(json)
# print the JSON string representation of the object
print(UserPreflightDTO.to_json())

# convert the object into a dict
user_preflight_dto_dict = user_preflight_dto_instance.to_dict()
# create an instance of UserPreflightDTO from a dict
user_preflight_dto_from_dict = UserPreflightDTO.from_dict(user_preflight_dto_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


