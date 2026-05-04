# Database


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**database_id** | **str** | This database&#39;s id | 
**user_id** | **str** | The id of the requesting user. The contents of the tree depends on the permissions of the requesting user. | 
**version** | **str** | The monotonically-increasing version of the database tree. The version number is incremented whenever a change is made that affects the database tree. | 
**label** | **str** | This database&#39;s human-readable label. | 
**description** | **str** |  | [optional] 
**owner_ref** | [**UserRef**](UserRef.md) |  | 
**language** | **str** | Current database language | [optional] 
**original_language** | **str** | Original database language | [optional] 
**languages** | **List[str]** | The list of languages that have been defined for this database, including the original language, if set, and any translations. | [optional] 
**suspended** | **bool** | True if this database is suspended for billing reasons | 
**role** | [**UserRole**](UserRole.md) | The requesting user&#39;s assigned role in this database. | 
**roles** | [**List[DatabaseRole]**](DatabaseRole.md) | The roles that have been defined for this database. | 
**storage** | **str** |  | 
**published_template** | **bool** | True if this database has been published as a template. | 
**security_categories** | [**List[SecurityCategory]**](SecurityCategory.md) | The security categories that have been defined for this database. | 
**resources** | [**List[DatabaseResource]**](DatabaseResource.md) | The set of resources (folders, forms, and subforms) that belong to this database. | 
**locks** | [**List[DatabaseLock]**](DatabaseLock.md) | The record locks that have been defined on this database. | [optional] 
**grants** | [**List[Grant]**](Grant.md) | The direct (non-role) permission grants that have been made to the requesting user for this database. | [optional] 
**billing_account_id** | **int** |  | 
**billing_plan** | **str** | The billing plan name under which this database falls. The billing plan can have an affect on which features are avialable within this database. | 

## Example

```python
from client.models.database import Database

# TODO update the JSON string below
json = "{}"
# create an instance of Database from a JSON string
database_instance = Database.from_json(json)
# print the JSON string representation of the object
print(Database.to_json())

# convert the object into a dict
database_dict = database_instance.to_dict()
# create an instance of Database from a dict
database_from_dict = Database.from_dict(database_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


