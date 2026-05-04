# QuerySort


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**formula** | **str** | The formula on which to sort. | 
**dir** | **str** | The direction to sort. | 

## Example

```python
from client.models.query_sort import QuerySort

# TODO update the JSON string below
json = "{}"
# create an instance of QuerySort from a JSON string
query_sort_instance = QuerySort.from_json(json)
# print the JSON string representation of the object
print(QuerySort.to_json())

# convert the object into a dict
query_sort_dict = query_sort_instance.to_dict()
# create an instance of QuerySort from a dict
query_sort_from_dict = QuerySort.from_dict(query_sort_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


