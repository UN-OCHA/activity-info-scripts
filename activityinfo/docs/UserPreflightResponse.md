# UserPreflightResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**user_id** | **str** |  | [optional] 
**activation_status** | **str** |  | 
**name** | **str** |  | [optional] 
**added_to_database** | **bool** |  | 
**valid_email** | **bool** |  | 
**localized_error_message** | **str** |  | [optional] 

## Example

```python
from client.models.user_preflight_response import UserPreflightResponse

# TODO update the JSON string below
json = "{}"
# create an instance of UserPreflightResponse from a JSON string
user_preflight_response_instance = UserPreflightResponse.from_json(json)
# print the JSON string representation of the object
print(UserPreflightResponse.to_json())

# convert the object into a dict
user_preflight_response_dict = user_preflight_response_instance.to_dict()
# create an instance of UserPreflightResponse from a dict
user_preflight_response_from_dict = UserPreflightResponse.from_dict(user_preflight_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


