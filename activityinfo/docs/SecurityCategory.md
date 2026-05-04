# SecurityCategory


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** |  | 
**label** | **str** |  | 

## Example

```python
from client.models.security_category import SecurityCategory

# TODO update the JSON string below
json = "{}"
# create an instance of SecurityCategory from a JSON string
security_category_instance = SecurityCategory.from_json(json)
# print the JSON string representation of the object
print(SecurityCategory.to_json())

# convert the object into a dict
security_category_dict = security_category_instance.to_dict()
# create an instance of SecurityCategory from a dict
security_category_from_dict = SecurityCategory.from_dict(security_category_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


