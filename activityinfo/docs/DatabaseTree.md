# DatabaseTree


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**database_id** | **str** |  | 
**user_id** | **str** |  | 
**version** | **str** |  | 
**label** | **str** |  | 
**description** | **str** |  | [optional] 
**owner_ref** | [**OwnerRef**](OwnerRef.md) |  | 
**language** | **str** |  | 
**original_language** | **str** |  | [optional] 
**languages** | **List[Optional[str]]** |  | [optional] [default to []]
**role** | [**DatabaseRole**](DatabaseRole.md) |  | 
**suspended** | **bool** |  | 
**billing_plan** | **str** |  | [optional] 
**storage** | **str** |  | 
**published_template** | **bool** |  | 
**resources** | [**List[Resource]**](Resource.md) |  | 
**grants** | [**List[Grant]**](Grant.md) |  | 
**locks** | **List[object]** |  | [optional] [default to []]
**roles** | [**List[Role]**](Role.md) |  | 
**security_categories** | **List[Dict[str, str]]** |  | 

## Example

```python
from client.models.database_tree import DatabaseTree

# TODO update the JSON string below
json = "{}"
# create an instance of DatabaseTree from a JSON string
database_tree_instance = DatabaseTree.from_json(json)
# print the JSON string representation of the object
print(DatabaseTree.to_json())

# convert the object into a dict
database_tree_dict = database_tree_instance.to_dict()
# create an instance of DatabaseTree from a dict
database_tree_from_dict = DatabaseTree.from_dict(database_tree_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


