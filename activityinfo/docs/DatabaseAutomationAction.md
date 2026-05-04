# DatabaseAutomationAction


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** | The type of action, e.g. &#39;WEBHOOK&#39; | 
**url** | **str** | The URL of the webhook action | [optional] 

## Example

```python
from client.models.database_automation_action import DatabaseAutomationAction

# TODO update the JSON string below
json = "{}"
# create an instance of DatabaseAutomationAction from a JSON string
database_automation_action_instance = DatabaseAutomationAction.from_json(json)
# print the JSON string representation of the object
print(DatabaseAutomationAction.to_json())

# convert the object into a dict
database_automation_action_dict = database_automation_action_instance.to_dict()
# create an instance of DatabaseAutomationAction from a dict
database_automation_action_from_dict = DatabaseAutomationAction.from_dict(database_automation_action_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


