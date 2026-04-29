# FieldTypeParametersUpdateDTO


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**units** | **str** |  | [optional] 
**input_mask** | **str** |  | [optional] 
**barcode** | **bool** |  | [optional] 
**cardinality** | **str** |  | [optional] 
**range** | **List[Dict[str, str]]** |  | [optional] 
**form_id** | **str** |  | [optional] 
**items** | [**List[FieldTypeParametersItemsUpdateDTO]**](FieldTypeParametersItemsUpdateDTO.md) |  | [optional] 
**formula** | **str** |  | [optional] 
**prefix_formula** | **str** |  | [optional] 
**lookup_configs** | [**List[TypeParameterLookupConfig]**](TypeParameterLookupConfig.md) |  | [optional] 
**aggregation** | **str** |  | [optional] 

## Example

```python
from client.models.field_type_parameters_update_dto import FieldTypeParametersUpdateDTO

# TODO update the JSON string below
json = "{}"
# create an instance of FieldTypeParametersUpdateDTO from a JSON string
field_type_parameters_update_dto_instance = FieldTypeParametersUpdateDTO.from_json(json)
# print the JSON string representation of the object
print(FieldTypeParametersUpdateDTO.to_json())

# convert the object into a dict
field_type_parameters_update_dto_dict = field_type_parameters_update_dto_instance.to_dict()
# create an instance of FieldTypeParametersUpdateDTO from a dict
field_type_parameters_update_dto_from_dict = FieldTypeParametersUpdateDTO.from_dict(field_type_parameters_update_dto_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


