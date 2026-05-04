# AddUserRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**email** | **str** | The email of the user to add | 
**name** | **str** | The name of the user to add. This is only used in the email invitation to the user if there is no existing ActivityInfo account with this email address. | 
**locale** | **str** | The locale to use when sending an email invite to the user, expressed as the two-letter ISO 639-1 code for the language, e.g. &#39;en&#39; for English. | 
**role** | [**UserRole**](UserRole.md) | The role to assign to the user. | 
**grants** | [**List[Grant]**](Grant.md) | Additional permissions to grant to the user. | [optional] 

## Example

```python
from client.models.add_user_request import AddUserRequest

# TODO update the JSON string below
json = "{}"
# create an instance of AddUserRequest from a JSON string
add_user_request_instance = AddUserRequest.from_json(json)
# print the JSON string representation of the object
print(AddUserRequest.to_json())

# convert the object into a dict
add_user_request_dict = add_user_request_instance.to_dict()
# create an instance of AddUserRequest from a dict
add_user_request_from_dict = AddUserRequest.from_dict(add_user_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


