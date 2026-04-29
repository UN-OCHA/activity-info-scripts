# QueryRowsRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**row_sources** | [**List[QueryRowsRowSource]**](QueryRowsRowSource.md) |  | [optional] 
**form_id** | **str** |  | [optional] 
**columns** | [**List[QueryRowsColumn]**](QueryRowsColumn.md) |  | 
**filter** | **str** |  | [optional] 
**filter_sets** | [**List[QueryRowsFilterSet]**](QueryRowsFilterSet.md) |  | [optional] 
**sort** | [**List[QueryRowsSort]**](QueryRowsSort.md) |  | [optional] 
**truncate_strings** | **bool** |  | [optional] 
**validation** | **Dict[str, object]** |  | [optional] 
**drafts** | **str** |  | [optional] 

## Example

```python
from client.models.query_rows_request import QueryRowsRequest

# TODO update the JSON string below
json = "{}"
# create an instance of QueryRowsRequest from a JSON string
query_rows_request_instance = QueryRowsRequest.from_json(json)
# print the JSON string representation of the object
print(QueryRowsRequest.to_json())

# convert the object into a dict
query_rows_request_dict = query_rows_request_instance.to_dict()
# create an instance of QueryRowsRequest from a dict
query_rows_request_from_dict = QueryRowsRequest.from_dict(query_rows_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


