# AuditDatabaseRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**resource_filter** | **str** | The id of a form, folder, or report. If provided, the result will only include events that relate to this resource. | [optional] 
**type_filter** | **List[str]** | Only include events of the given types. | [optional] 
**start_time** | **int** | The start time of the request, in milliseconds since the unix epoch. The results will include the first 100 - 150 events that occurred before this time. | 
**end_time** | **int** | The end time of the request, in milliseconds since the unix epoch. The results will include the events that occurred after the end time and before the start time. | [optional] 

## Example

```python
from client.models.audit_database_request import AuditDatabaseRequest

# TODO update the JSON string below
json = "{}"
# create an instance of AuditDatabaseRequest from a JSON string
audit_database_request_instance = AuditDatabaseRequest.from_json(json)
# print the JSON string representation of the object
print(AuditDatabaseRequest.to_json())

# convert the object into a dict
audit_database_request_dict = audit_database_request_instance.to_dict()
# create an instance of AuditDatabaseRequest from a dict
audit_database_request_from_dict = AuditDatabaseRequest.from_dict(audit_database_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


