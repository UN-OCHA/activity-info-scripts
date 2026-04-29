# FieldTypeParametersItemsUpdateDTO


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** |  | 
**label** | **str** |  | 

## Example

```python
from client.models.field_type_parameters_items_update_dto import FieldTypeParametersItemsUpdateDTO

# TODO update the JSON string below
json = "{}"
# create an instance of FieldTypeParametersItemsUpdateDTO from a JSON string
field_type_parameters_items_update_dto_instance = FieldTypeParametersItemsUpdateDTO.from_json(json)
# print the JSON string representation of the object
print(FieldTypeParametersItemsUpdateDTO.to_json())

# convert the object into a dict
field_type_parameters_items_update_dto_dict = field_type_parameters_items_update_dto_instance.to_dict()
# create an instance of FieldTypeParametersItemsUpdateDTO from a dict
field_type_parameters_items_update_dto_from_dict = FieldTypeParametersItemsUpdateDTO.from_dict(field_type_parameters_items_update_dto_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


