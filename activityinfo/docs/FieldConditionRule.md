# FieldConditionRule

An individual rule for a field-level condition associated with a Grant for a database resource.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**object_type** | **str** | The object type for this rule, e.g. &#39;FIELD&#39; | 
**predicate_type** | **str** | The predicate for this rule, e.g. &#39;IS&#39; or &#39;ISNOT&#39; | 
**value** | **str** | The comparison value for this rule, e.g. the field id | 

## Example

```python
from client.models.field_condition_rule import FieldConditionRule

# TODO update the JSON string below
json = "{}"
# create an instance of FieldConditionRule from a JSON string
field_condition_rule_instance = FieldConditionRule.from_json(json)
# print the JSON string representation of the object
print(FieldConditionRule.to_json())

# convert the object into a dict
field_condition_rule_dict = field_condition_rule_instance.to_dict()
# create an instance of FieldConditionRule from a dict
field_condition_rule_from_dict = FieldConditionRule.from_dict(field_condition_rule_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


