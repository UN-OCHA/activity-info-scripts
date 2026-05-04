# QuerySource


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**root_form_id** | **str** | CUID of the root form to query. | 

## Example

```python
from client.models.query_source import QuerySource

# TODO update the JSON string below
json = "{}"
# create an instance of QuerySource from a JSON string
query_source_instance = QuerySource.from_json(json)
# print the JSON string representation of the object
print(QuerySource.to_json())

# convert the object into a dict
query_source_dict = query_source_instance.to_dict()
# create an instance of QuerySource from a dict
query_source_from_dict = QuerySource.from_dict(query_source_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


