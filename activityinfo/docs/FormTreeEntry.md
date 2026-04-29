# FormTreeEntry


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** |  | 
**var_schema** | [**FormSchema**](FormSchema.md) |  | 
**schema_version** | **int** |  | 

## Example

```python
from client.models.form_tree_entry import FormTreeEntry

# TODO update the JSON string below
json = "{}"
# create an instance of FormTreeEntry from a JSON string
form_tree_entry_instance = FormTreeEntry.from_json(json)
# print the JSON string representation of the object
print(FormTreeEntry.to_json())

# convert the object into a dict
form_tree_entry_dict = form_tree_entry_instance.to_dict()
# create an instance of FormTreeEntry from a dict
form_tree_entry_from_dict = FormTreeEntry.from_dict(form_tree_entry_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


