# DatabaseAutomation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**automation_id** | **str** | The automation&#39;s id | 
**label** | **str** | A human-readable label for this automation | [optional] 
**resource_id** | **str** | The resource (form, or subform) to which this automation applies. | [optional] 
**trigger** | **str** | The trigger that applies to this automation. Values: RECORD_ADDED, RECORD_EDITED, RECORD_DELETED | [optional] 
**trigger_formula** | **str** | The formula to which this automation applies. currently not supported | [optional] 
**action** | [**DatabaseAutomationAction**](DatabaseAutomationAction.md) | The action to be performed by this automation | [optional] 
**active** | **bool** | Determines whether this automation is active or not | [optional] 

## Example

```python
from client.models.database_automation import DatabaseAutomation

# TODO update the JSON string below
json = "{}"
# create an instance of DatabaseAutomation from a JSON string
database_automation_instance = DatabaseAutomation.from_json(json)
# print the JSON string representation of the object
print(DatabaseAutomation.to_json())

# convert the object into a dict
database_automation_dict = database_automation_instance.to_dict()
# create an instance of DatabaseAutomation from a dict
database_automation_from_dict = DatabaseAutomation.from_dict(database_automation_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


