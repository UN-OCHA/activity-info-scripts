# StageImportResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**upload_url** | **str** | Pre-signed URL to upload staged import payload. | 
**import_id** | **str** | Identifier used when starting an importRecords job. | 

## Example

```python
from client.models.stage_import_response import StageImportResponse

# TODO update the JSON string below
json = "{}"
# create an instance of StageImportResponse from a JSON string
stage_import_response_instance = StageImportResponse.from_json(json)
# print the JSON string representation of the object
print(StageImportResponse.to_json())

# convert the object into a dict
stage_import_response_dict = stage_import_response_instance.to_dict()
# create an instance of StageImportResponse from a dict
stage_import_response_from_dict = StageImportResponse.from_dict(stage_import_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


