# AddFormRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**form_resource** | [**DatabaseResource**](DatabaseResource.md) |  | 
**form_class** | [**FormSchema**](FormSchema.md) |  | 

## Example

```python
from client.models.add_form_request import AddFormRequest

# TODO update the JSON string below
json = "{}"
# create an instance of AddFormRequest from a JSON string
add_form_request_instance = AddFormRequest.from_json(json)
# print the JSON string representation of the object
print(AddFormRequest.to_json())

# convert the object into a dict
add_form_request_dict = add_form_request_instance.to_dict()
# create an instance of AddFormRequest from a dict
add_form_request_from_dict = AddFormRequest.from_dict(add_form_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


