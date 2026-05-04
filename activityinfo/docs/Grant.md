# Grant

A set of permissions for a single database resource (database, folder, form, subform) that is either associated with a role or given directly to a user.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**resource_id** | **str** | the database, folder, form or subform being granted access to. | 
**optional** | **bool** | An optional resource is granted selectively to individual users assigned to a role. | [default to False]
**operations** | [**List[Permission]**](Permission.md) | The set of operations (add/edit/delete etc.) and their record-level conditions permitted on this resource | 
**conditions** | [**List[FieldCondition]**](FieldCondition.md) | The set of field-level conditions applied to this resource | [optional] 

## Example

```python
from client.models.grant import Grant

# TODO update the JSON string below
json = "{}"
# create an instance of Grant from a JSON string
grant_instance = Grant.from_json(json)
# print the JSON string representation of the object
print(Grant.to_json())

# convert the object into a dict
grant_dict = grant_instance.to_dict()
# create an instance of Grant from a dict
grant_from_dict = Grant.from_dict(grant_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


