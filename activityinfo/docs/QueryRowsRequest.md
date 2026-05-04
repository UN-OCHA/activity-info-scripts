# QueryRowsRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**row_sources** | [**List[QuerySource]**](QuerySource.md) | Exactly one RowSource is required. | 
**form_id** | **str** | CUID of the root form to query. This is deprecated in favour of rowSources | [optional] 
**columns** | [**List[QueryColumn]**](QueryColumn.md) | The list of columns to retrieve | 
**filter** | **str** | A boolean-valued ActivityInfo formula to filter the records included in this query. Resulting rows must match both the filter formula, if provided, and all the provided filterSets. Filter sets are more efficient than formulas when you have a large number of values. | [optional] 
**filter_sets** | [**List[QueryFilterSet]**](QueryFilterSet.md) | Set-based filters to apply to records returned by the query, each of which is a pair of a formula and a set of values to include. Resulting rows must match both the filter formula, if provided, and all the provided filterSets. Filter sets are more efficient than formulas when you have a large number of values. | [optional] 
**sort** | [**List[QuerySort]**](QuerySort.md) | Sort instructions specified with a formula and a direction. Only one sort instruction can be specified, so sorting on multiple columns must be done with a single formula expression. | [optional] 
**truncate_strings** | **bool** |  | [optional] 
**validation** | **Dict[str, object]** |  | [optional] 
**drafts** | **str** |  | [optional] 
**tags** | **List[str]** |  | [optional] 

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


