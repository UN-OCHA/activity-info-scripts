# Form


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | The id of the form | 
**var_schema** | [**FormSchema**](FormSchema.md) |  | 
**schema_version** | **int** |  | 
**permissions** | [**FormPermissions**](FormPermissions.md) |  | 
**visible** | **bool** | True if the form is visible to the authenticated user | [optional] 
**deleted** | **bool** | True if the form has been deleted | [optional] 
**suspended** | **bool** | True if the form belongs to a suspended database. | [optional] 

## Example

```python
from client.models.form import Form

# TODO update the JSON string below
json = "{}"
# create an instance of Form from a JSON string
form_instance = Form.from_json(json)
# print the JSON string representation of the object
print(Form.to_json())

# convert the object into a dict
form_dict = form_instance.to_dict()
# create an instance of Form from a dict
form_from_dict = Form.from_dict(form_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


