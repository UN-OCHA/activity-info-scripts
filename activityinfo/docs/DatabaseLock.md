# DatabaseLock

A record lock that has been defined for a database

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | This lock&#39;s id. | 
**label** | **str** | A human-readable label for this lock | 
**resource_id** | **str** | The resource (database, folder, form, or subform) to which this lock applies. | 
**date_range** | [**DatabaseLockDateRange**](DatabaseLockDateRange.md) | The date range to which this lock applies, if it is a date-range lock. | [optional] 
**formula** | **str** | The formula to which this lock applies, if it is a rule-based lock. | [optional] 
**message** | **str** | user description of the rule-based lock. | [optional] 
**deactivated** | **bool** | Whether or not this lock is active. True when deactivated | [default to False]

## Example

```python
from client.models.database_lock import DatabaseLock

# TODO update the JSON string below
json = "{}"
# create an instance of DatabaseLock from a JSON string
database_lock_instance = DatabaseLock.from_json(json)
# print the JSON string representation of the object
print(DatabaseLock.to_json())

# convert the object into a dict
database_lock_dict = database_lock_instance.to_dict()
# create an instance of DatabaseLock from a dict
database_lock_from_dict = DatabaseLock.from_dict(database_lock_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


