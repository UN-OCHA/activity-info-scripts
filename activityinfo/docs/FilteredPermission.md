# FilteredPermission


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**operation** | **str** |  | 
**filter** | **str** |  | [optional] 
**security_categories** | **List[str]** |  | [optional] 

## Example

```python
from client.models.filtered_permission import FilteredPermission

# TODO update the JSON string below
json = "{}"
# create an instance of FilteredPermission from a JSON string
filtered_permission_instance = FilteredPermission.from_json(json)
# print the JSON string representation of the object
print(FilteredPermission.to_json())

# convert the object into a dict
filtered_permission_dict = filtered_permission_instance.to_dict()
# create an instance of FilteredPermission from a dict
filtered_permission_from_dict = FilteredPermission.from_dict(filtered_permission_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


