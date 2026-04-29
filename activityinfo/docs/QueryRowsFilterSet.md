# QueryRowsFilterSet


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**column_formula** | **str** |  | 
**values** | **List[str]** |  | 

## Example

```python
from client.models.query_rows_filter_set import QueryRowsFilterSet

# TODO update the JSON string below
json = "{}"
# create an instance of QueryRowsFilterSet from a JSON string
query_rows_filter_set_instance = QueryRowsFilterSet.from_json(json)
# print the JSON string representation of the object
print(QueryRowsFilterSet.to_json())

# convert the object into a dict
query_rows_filter_set_dict = query_rows_filter_set_instance.to_dict()
# create an instance of QueryRowsFilterSet from a dict
query_rows_filter_set_from_dict = QueryRowsFilterSet.from_dict(query_rows_filter_set_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


