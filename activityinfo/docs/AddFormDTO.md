# AddFormDTO


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**form_resource** | [**FormResource**](FormResource.md) |  | 
**form_class** | [**FormClass**](FormClass.md) |  | 

## Example

```python
from client.models.add_form_dto import AddFormDTO

# TODO update the JSON string below
json = "{}"
# create an instance of AddFormDTO from a JSON string
add_form_dto_instance = AddFormDTO.from_json(json)
# print the JSON string representation of the object
print(AddFormDTO.to_json())

# convert the object into a dict
add_form_dto_dict = add_form_dto_instance.to_dict()
# create an instance of AddFormDTO from a dict
add_form_dto_from_dict = AddFormDTO.from_dict(add_form_dto_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


