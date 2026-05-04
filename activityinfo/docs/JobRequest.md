# JobRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** | The type of the job to start. | [optional] 
**descriptor** | **Dict[str, object]** | The object containing the elements required for the specific type of job | [optional] 

## Example

```python
from client.models.job_request import JobRequest

# TODO update the JSON string below
json = "{}"
# create an instance of JobRequest from a JSON string
job_request_instance = JobRequest.from_json(json)
# print the JSON string representation of the object
print(JobRequest.to_json())

# convert the object into a dict
job_request_dict = job_request_instance.to_dict()
# create an instance of JobRequest from a dict
job_request_from_dict = JobRequest.from_dict(job_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


