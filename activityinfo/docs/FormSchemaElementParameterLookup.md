# FormSchemaElementParameterLookup


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | The immutable CUID of this lookup item. Must match the format [a-z][a-z0-9]{0,25} and be unique within this field. | 
**formula** | **str** | An ActivityInfo formula specifying the field or expression being looked up. | 
**lookup_label** | **str** | Human-readable label for this lookup item. | 

## Example

```python
from client.models.form_schema_element_parameter_lookup import FormSchemaElementParameterLookup

# TODO update the JSON string below
json = "{}"
# create an instance of FormSchemaElementParameterLookup from a JSON string
form_schema_element_parameter_lookup_instance = FormSchemaElementParameterLookup.from_json(json)
# print the JSON string representation of the object
print(FormSchemaElementParameterLookup.to_json())

# convert the object into a dict
form_schema_element_parameter_lookup_dict = form_schema_element_parameter_lookup_instance.to_dict()
# create an instance of FormSchemaElementParameterLookup from a dict
form_schema_element_parameter_lookup_from_dict = FormSchemaElementParameterLookup.from_dict(form_schema_element_parameter_lookup_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


