# DatabaseAudit


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**events** | [**List[DatabaseAuditEvents]**](DatabaseAuditEvents.md) | List of matching auditable events. | [optional] 
**start_time** | **int** | The start time of the request, in milliseconds since the unix epoch. The results will include the first 100 - 150 events that occurred before this time. | 
**end_time** | **int** | The end time of the request, in milliseconds since the unix epoch. The results will include the events that occurred after the end time and before the start time. | 
**more_events** | **bool** | True if there are more events earlier than endTime available, but not included in this response. Submit a new request with a startTime equal to the endTime to fetch the next batch. | 

## Example

```python
from client.models.database_audit import DatabaseAudit

# TODO update the JSON string below
json = "{}"
# create an instance of DatabaseAudit from a JSON string
database_audit_instance = DatabaseAudit.from_json(json)
# print the JSON string representation of the object
print(DatabaseAudit.to_json())

# convert the object into a dict
database_audit_dict = database_audit_instance.to_dict()
# create an instance of DatabaseAudit from a dict
database_audit_from_dict = DatabaseAudit.from_dict(database_audit_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


