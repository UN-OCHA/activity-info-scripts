# RecordUpdateRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**changes** | [**List[RecordUpdateChange]**](RecordUpdateChange.md) |  | 
**from_import_context** | **bool** | True if update request comes from bulk importer | [optional] 

## Example

```python
from client.models.record_update_request import RecordUpdateRequest

# TODO update the JSON string below
json = "{}"
# create an instance of RecordUpdateRequest from a JSON string
record_update_request_instance = RecordUpdateRequest.from_json(json)
# print the JSON string representation of the object
print(RecordUpdateRequest.to_json())

# convert the object into a dict
record_update_request_dict = record_update_request_instance.to_dict()
# create an instance of RecordUpdateRequest from a dict
record_update_request_from_dict = RecordUpdateRequest.from_dict(record_update_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


