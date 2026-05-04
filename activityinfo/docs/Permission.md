# Permission

An individual operation (add/edit/view) and any associated record-level condition, either associated with a Grant for a specific database resource, or assigned to a role directly for higher-level database operations.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**operation** | [**AnyOf**](AnyOf.md) | One of a number of predetermined values | 
**filter** | **str** | A formula which restricts a permission to specific records. Only record-level permissions can be filtered. | [optional] 
**security_categories** | [**List[SecurityCategory]**](SecurityCategory.md) | The security categories that have been defined for this permission. | [optional] 

## Example

```python
from client.models.permission import Permission

# TODO update the JSON string below
json = "{}"
# create an instance of Permission from a JSON string
permission_instance = Permission.from_json(json)
# print the JSON string representation of the object
print(Permission.to_json())

# convert the object into a dict
permission_dict = permission_instance.to_dict()
# create an instance of Permission from a dict
permission_from_dict = Permission.from_dict(permission_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


