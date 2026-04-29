# FormClass


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** |  | 
**label** | **str** |  | 
**schema_version** | **int** |  | 
**database_id** | **str** |  | 
**parent_form_id** | **str** |  | [optional] 
**record_label_field_id** | **str** |  | [optional] 
**elements** | [**List[SchemaFieldDTO]**](SchemaFieldDTO.md) |  | 

## Example

```python
from client.models.form_class import FormClass

# TODO update the JSON string below
json = "{}"
# create an instance of FormClass from a JSON string
form_class_instance = FormClass.from_json(json)
# print the JSON string representation of the object
print(FormClass.to_json())

# convert the object into a dict
form_class_dict = form_class_instance.to_dict()
# create an instance of FormClass from a dict
form_class_from_dict = FormClass.from_dict(form_class_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


