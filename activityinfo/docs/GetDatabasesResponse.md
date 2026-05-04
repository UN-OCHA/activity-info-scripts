# GetDatabasesResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**database_id** | **str** | The globally-unique id of this database. | 
**label** | **str** | The human-readable name of this database. | 
**description** | **str** | A longer, optional description for this database. | [optional] 
**owner_id** | **str** | The id of the user who owns this database. | [optional] 
**billing_account_id** | **int** | The id of the billing account to which this database belongs | [optional] 
**suspended** | **bool** | True if this database belongs to a billing account that has been suspended. | [optional] 
**published_template** | **bool** | True if this database has been published as a template. | [optional] 
**languages** | **List[str]** | The list of languages that have been defined for this database, including the original language, if set, and any translations. | [optional] 

## Example

```python
from client.models.get_databases_response import GetDatabasesResponse

# TODO update the JSON string below
json = "{}"
# create an instance of GetDatabasesResponse from a JSON string
get_databases_response_instance = GetDatabasesResponse.from_json(json)
# print the JSON string representation of the object
print(GetDatabasesResponse.to_json())

# convert the object into a dict
get_databases_response_dict = get_databases_response_instance.to_dict()
# create an instance of GetDatabasesResponse from a dict
get_databases_response_from_dict = GetDatabasesResponse.from_dict(get_databases_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


