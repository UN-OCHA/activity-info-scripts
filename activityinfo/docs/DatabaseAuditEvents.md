# DatabaseAuditEvents


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | An opaque, id uniquely identifying this event within this result set. | 
**time** | **int** | The time of this event, in seconds since 1970-01-01. | 
**user** | [**UserRef**](UserRef.md) | The user who initiated this event. | [optional] 
**record_ref** | **str** | The record ref, if this event concerns a record. | [optional] 
**form_id** | **str** | The form id, if this event concerns a form. | [optional] 
**version** | **int** | The version number of the affected resource. | [optional] 
**database_user_id** | **str** | For user permission events, this is the id of the user who was affected. | [optional] 
**description** | **str** | A human-readable description of the event. | 
**type** | **str** | The type of event. Values: DATABASE_TREE, USER, RECORD, FORM, AUTOMATION | 
**resource_types** | **List[str]** | One or more type(s) of the resource(s) affected | 
**resource_id** | **str** | Id of the affected resource OR id of the database if more than one resource type is present | 
**added** | **bool** | True if this event concerns an addition. | 
**updated** | **bool** | True if this event concerns an update. | 
**deleted** | **bool** | True if this event concerns an deletion. | 
**recovered** | **bool** | True if this event concerns a recovery of data. | 
**reverted** | **bool** | True if this event has been reverted by an administrator. | 
**automation** | [**DatabaseAutomation**](DatabaseAutomation.md) | The automation, if this event concerns an automation. | [optional] 
**merge_id** | **str** | The merge id, if this event concerns a merge of records. | [optional] 

## Example

```python
from client.models.database_audit_events import DatabaseAuditEvents

# TODO update the JSON string below
json = "{}"
# create an instance of DatabaseAuditEvents from a JSON string
database_audit_events_instance = DatabaseAuditEvents.from_json(json)
# print the JSON string representation of the object
print(DatabaseAuditEvents.to_json())

# convert the object into a dict
database_audit_events_dict = database_audit_events_instance.to_dict()
# create an instance of DatabaseAuditEvents from a dict
database_audit_events_from_dict = DatabaseAuditEvents.from_dict(database_audit_events_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


