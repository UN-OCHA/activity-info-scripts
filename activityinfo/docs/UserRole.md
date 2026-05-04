# UserRole


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | The id of the role assigned to this user. | 
**parameters** | **Dict[str, object]** | The user&#39;s values of the parameters defined for this role. | [optional] 
**resources** | **List[str]** | The role&#39;s optional resources (database, folder, forms, or subforms) granted to this user. | [optional] 

## Example

```python
from client.models.user_role import UserRole

# TODO update the JSON string below
json = "{}"
# create an instance of UserRole from a JSON string
user_role_instance = UserRole.from_json(json)
# print the JSON string representation of the object
print(UserRole.to_json())

# convert the object into a dict
user_role_dict = user_role_instance.to_dict()
# create an instance of UserRole from a dict
user_role_from_dict = UserRole.from_dict(user_role_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


