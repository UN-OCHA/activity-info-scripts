# AddDatabaseRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | The id of the new database. | 
**label** | **str** | Human readable name of the database. | 
**template_id** | **str** | The id of the template to use in creating the new database. The the two templates currently supported are \&quot;reporting\&quot; (Multi-partner reporting) and \&quot;casemanagement.\&quot; If ommitted, a blank database with three simple roles will be added. | [optional] 
**description** | **str** | An optional, longer description for the database. | [optional] 

## Example

```python
from client.models.add_database_request import AddDatabaseRequest

# TODO update the JSON string below
json = "{}"
# create an instance of AddDatabaseRequest from a JSON string
add_database_request_instance = AddDatabaseRequest.from_json(json)
# print the JSON string representation of the object
print(AddDatabaseRequest.to_json())

# convert the object into a dict
add_database_request_dict = add_database_request_instance.to_dict()
# create an instance of AddDatabaseRequest from a dict
add_database_request_from_dict = AddDatabaseRequest.from_dict(add_database_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


