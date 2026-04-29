# FormTree


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**root** | **str** |  | 
**forms** | [**Dict[str, FormTreeEntry]**](FormTreeEntry.md) |  | 

## Example

```python
from client.models.form_tree import FormTree

# TODO update the JSON string below
json = "{}"
# create an instance of FormTree from a JSON string
form_tree_instance = FormTree.from_json(json)
# print the JSON string representation of the object
print(FormTree.to_json())

# convert the object into a dict
form_tree_dict = form_tree_instance.to_dict()
# create an instance of FormTree from a dict
form_tree_from_dict = FormTree.from_dict(form_tree_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


