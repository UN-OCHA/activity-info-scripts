# FieldCondition

A field-level condition associated with a Grant for a single database resource (database, folder, form, subform)

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**operations** | **List[str]** | The field operations (view and/or edit) allowed for this condition | 
**criteria** | **str** | Whether the condition requires all or any of the condition&#39;s rules to be met | 
**rules** | [**List[FieldConditionRule]**](FieldConditionRule.md) | The condition&#39;s rules | 

## Example

```python
from client.models.field_condition import FieldCondition

# TODO update the JSON string below
json = "{}"
# create an instance of FieldCondition from a JSON string
field_condition_instance = FieldCondition.from_json(json)
# print the JSON string representation of the object
print(FieldCondition.to_json())

# convert the object into a dict
field_condition_dict = field_condition_instance.to_dict()
# create an instance of FieldCondition from a dict
field_condition_from_dict = FieldCondition.from_dict(field_condition_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


