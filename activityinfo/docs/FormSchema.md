# FormSchema


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** |  | 
**schema_version** | **int** |  | 
**database_id** | **str** |  | 
**parent_form_id** | **str** |  | [optional] 
**label** | **str** |  | 
**record_label_field_id** | **str** |  | [optional] 
**elements** | [**List[SchemaFieldDTO]**](SchemaFieldDTO.md) |  | 

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


