# QueryRowsColumn


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** |  | 
**formula** | **str** |  | [optional] 
**expression** | **str** |  | [optional] 

## Example

```python
from client.models.query_rows_column import QueryRowsColumn

# TODO update the JSON string below
json = "{}"
# create an instance of QueryRowsColumn from a JSON string
query_rows_column_instance = QueryRowsColumn.from_json(json)
# print the JSON string representation of the object
print(QueryRowsColumn.to_json())

# convert the object into a dict
query_rows_column_dict = query_rows_column_instance.to_dict()
# create an instance of QueryRowsColumn from a dict
query_rows_column_from_dict = QueryRowsColumn.from_dict(query_rows_column_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


