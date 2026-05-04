# FormSchema


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | The immutable CUID for this form. Must be globally unique within an ActivityInfo server | 
**label** | **str** | A human-readable label for this form | 
**schema_version** | **int** | A monotonically increasing version number of this schema assigned by the server upon updates | 
**database_id** | **str** | The id of the database to which this form belongs | 
**parent_form_id** | **str** | The id of this form&#39;s parent, if this form is a subform. Note that subforms can only be created by adding a subform field to the parent form. | [optional] 
**record_label_field_id** | **str** |  | [optional] 
**elements** | [**List[FormSchemaElement]**](FormSchemaElement.md) | This form&#39;s fields, section headers, and other elements. | 

## Example

```python
from client.models.form_schema import FormSchema

# TODO update the JSON string below
json = "{}"
# create an instance of FormSchema from a JSON string
form_schema_instance = FormSchema.from_json(json)
# print the JSON string representation of the object
print(FormSchema.to_json())

# convert the object into a dict
form_schema_dict = form_schema_instance.to_dict()
# create an instance of FormSchema from a dict
form_schema_from_dict = FormSchema.from_dict(form_schema_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


