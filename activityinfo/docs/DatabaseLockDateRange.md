# DatabaseLockDateRange

The start and end date for a date-range record lock on a database

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**start_date** | **date** |  | 
**end_date** | **date** |  | 

## Example

```python
from client.models.database_lock_date_range import DatabaseLockDateRange

# TODO update the JSON string below
json = "{}"
# create an instance of DatabaseLockDateRange from a JSON string
database_lock_date_range_instance = DatabaseLockDateRange.from_json(json)
# print the JSON string representation of the object
print(DatabaseLockDateRange.to_json())

# convert the object into a dict
database_lock_date_range_dict = database_lock_date_range_instance.to_dict()
# create an instance of DatabaseLockDateRange from a dict
database_lock_date_range_from_dict = DatabaseLockDateRange.from_dict(database_lock_date_range_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


