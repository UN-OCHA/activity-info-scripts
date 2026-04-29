# SchemaFieldDTO


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** |  | 
**code** | **str** |  | 
**label** | **str** |  | 
**description** | **str** |  | [optional] 
**relevance_condition** | **str** |  | [optional] 
**validation_condition** | **str** |  | [optional] 
**data_entry_visible** | **bool** |  | [optional] [default to True]
**table_visible** | **bool** |  | [optional] [default to True]
**required** | **bool** |  | 
**key** | **bool** |  | [optional] 
**unique** | **bool** |  | [optional] 
**read_only** | **bool** |  | [optional] 
**default_value_formula** | **str** |  | [optional] 
**type** | **str** |  | 
**type_parameters** | [**FieldTypeParametersUpdateDTO**](FieldTypeParametersUpdateDTO.md) |  | [optional] 

## Example

```python
from client.models.schema_field_dto import SchemaFieldDTO

# TODO update the JSON string below
json = "{}"
# create an instance of SchemaFieldDTO from a JSON string
schema_field_dto_instance = SchemaFieldDTO.from_json(json)
# print the JSON string representation of the object
print(SchemaFieldDTO.to_json())

# convert the object into a dict
schema_field_dto_dict = schema_field_dto_instance.to_dict()
# create an instance of SchemaFieldDTO from a dict
schema_field_dto_from_dict = SchemaFieldDTO.from_dict(schema_field_dto_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


