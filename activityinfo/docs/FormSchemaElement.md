# FormSchemaElement


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | An immutable CUID for this field that is unique within the form | 
**code** | **str** | A developer-friendly code for this field that can be used in formulas and in the API. Must match the pattern ^[A-Za-z][A-Za-z0-9_]* | [optional] 
**label** | **str** | A short human-friendly label for this field | 
**description** | **str** | An optional more detailed sub-label for this field | [optional] 
**relevance_condition** | **str** | A boolean-valued ActivityInfo formula that determines when this field is &#39;relevant&#39;. Fields that are not relevant are not displayed during data entry and must be blank. | [optional] 
**validation_condition** | **str** | A boolean-valued ActivityInfo formula that determines when this field&#39;s value is valid. | [optional] 
**data_entry_visible** | **bool** | If false, this field is not shown in data entry | [optional] [default to True]
**table_visible** | **bool** | If false, this field is not shown by default in the table view | [optional] [default to True]
**required** | **bool** | If true, a value for this field must be provided | [default to False]
**key** | **bool** | If true, this field is part of the form&#39;s natural key fields whose combination must be unique within the form | [default to False]
**unique** | **bool** | If true, the value for this field must be unique across the form&#39;s records | [optional] [default to False]
**read_only** | **bool** | If true, the value for this field cannot be modified by any user (but it can be set by default on record add). | [optional] [default to False]
**default_value** | **str** | The default value for this field, set on record add | [optional] 
**default_value_formula** | **str** | An ActivityInfo formula that generates the default value for this field, set on record add | [optional] 
**type** | **str** | The field type. | 
**security_category_id** | **str** |  | [optional] 
**type_parameters** | [**FormSchemaElementParameter**](FormSchemaElementParameter.md) | Additional type-specific properties of this field. | [optional] 

## Example

```python
from client.models.form_schema_element import FormSchemaElement

# TODO update the JSON string below
json = "{}"
# create an instance of FormSchemaElement from a JSON string
form_schema_element_instance = FormSchemaElement.from_json(json)
# print the JSON string representation of the object
print(FormSchemaElement.to_json())

# convert the object into a dict
form_schema_element_dict = form_schema_element_instance.to_dict()
# create an instance of FormSchemaElement from a dict
form_schema_element_from_dict = FormSchemaElement.from_dict(form_schema_element_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


