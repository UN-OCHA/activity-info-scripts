# FormResource


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** |  | 
**type** | **str** |  | 
**parent_id** | **str** |  | 
**label** | **str** |  | 
**visibility** | **str** |  | [optional] 

## Example

```python
from client.models.form_resource import FormResource

# TODO update the JSON string below
json = "{}"
# create an instance of FormResource from a JSON string
form_resource_instance = FormResource.from_json(json)
# print the JSON string representation of the object
print(FormResource.to_json())

# convert the object into a dict
form_resource_dict = form_resource_instance.to_dict()
# create an instance of FormResource from a dict
form_resource_from_dict = FormResource.from_dict(form_resource_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


