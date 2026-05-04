# AddUpdateFormResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**database** | [**Database**](Database.md) |  | 
**forms** | [**List[Form]**](Form.md) |  | 

## Example

```python
from client.models.add_update_form_response import AddUpdateFormResponse

# TODO update the JSON string below
json = "{}"
# create an instance of AddUpdateFormResponse from a JSON string
add_update_form_response_instance = AddUpdateFormResponse.from_json(json)
# print the JSON string representation of the object
print(AddUpdateFormResponse.to_json())

# convert the object into a dict
add_update_form_response_dict = add_update_form_response_instance.to_dict()
# create an instance of AddUpdateFormResponse from a dict
add_update_form_response_from_dict = AddUpdateFormResponse.from_dict(add_update_form_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


