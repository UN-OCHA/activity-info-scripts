# QueryColumn


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | The id of the column. This is the column name used to identify the column in the resulting table. | 
**formula** | **str** | An ActivityInfo formula that calculates the value shown in the column. formula and expression are interchangeable, but formula is given precedence. | [optional] 
**expression** | **str** | An ActivityInfo formula that calculates the value shown in the column. formula and expression are interchangeable, but formula is given precedence. | [optional] 

## Example

```python
from client.models.query_column import QueryColumn

# TODO update the JSON string below
json = "{}"
# create an instance of QueryColumn from a JSON string
query_column_instance = QueryColumn.from_json(json)
# print the JSON string representation of the object
print(QueryColumn.to_json())

# convert the object into a dict
query_column_dict = query_column_instance.to_dict()
# create an instance of QueryColumn from a dict
query_column_from_dict = QueryColumn.from_dict(query_column_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


