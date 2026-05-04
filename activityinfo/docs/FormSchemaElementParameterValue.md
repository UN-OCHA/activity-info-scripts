# FormSchemaElementParameterValue


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | The immutable CUID of this selection item. Must match the format [a-z][a-z0-9]{0,25} and be unique within this field. | 
**label** | **str** | Human-readable label for this selection item. | 

## Example

```python
from client.models.form_schema_element_parameter_value import FormSchemaElementParameterValue

# TODO update the JSON string below
json = "{}"
# create an instance of FormSchemaElementParameterValue from a JSON string
form_schema_element_parameter_value_instance = FormSchemaElementParameterValue.from_json(json)
# print the JSON string representation of the object
print(FormSchemaElementParameterValue.to_json())

# convert the object into a dict
form_schema_element_parameter_value_dict = form_schema_element_parameter_value_instance.to_dict()
# create an instance of FormSchemaElementParameterValue from a dict
form_schema_element_parameter_value_from_dict = FormSchemaElementParameterValue.from_dict(form_schema_element_parameter_value_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


