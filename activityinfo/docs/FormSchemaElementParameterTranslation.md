# FormSchemaElementParameterTranslation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**field_id** | **str** | The id of the field of which this field is a translation. Both fields must be of the same type. | 
**languages** | **List[str]** | The language of the translation. | 

## Example

```python
from client.models.form_schema_element_parameter_translation import FormSchemaElementParameterTranslation

# TODO update the JSON string below
json = "{}"
# create an instance of FormSchemaElementParameterTranslation from a JSON string
form_schema_element_parameter_translation_instance = FormSchemaElementParameterTranslation.from_json(json)
# print the JSON string representation of the object
print(FormSchemaElementParameterTranslation.to_json())

# convert the object into a dict
form_schema_element_parameter_translation_dict = form_schema_element_parameter_translation_instance.to_dict()
# create an instance of FormSchemaElementParameterTranslation from a dict
form_schema_element_parameter_translation_from_dict = FormSchemaElementParameterTranslation.from_dict(form_schema_element_parameter_translation_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


