# JobStatus


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | The job&#39;s id | [optional] 
**user_id** | **str** | The id of the user who started the job | [optional] 
**descriptor** | **Dict[str, object]** | The job descriptor used to start this job | [optional] 
**state** | [**AnyOf**](AnyOf.md) | The job&#39;s current state | [optional] 
**job_result** | **Dict[str, object]** |  | [optional] 
**percent_complete** | **float** |  | [optional] 
**error** | [**MessageResponse**](MessageResponse.md) | If the job state is FAILED, the reason for the failure. | [optional] 

## Example

```python
from client.models.job_status import JobStatus

# TODO update the JSON string below
json = "{}"
# create an instance of JobStatus from a JSON string
job_status_instance = JobStatus.from_json(json)
# print the JSON string representation of the object
print(JobStatus.to_json())

# convert the object into a dict
job_status_dict = job_status_instance.to_dict()
# create an instance of JobStatus from a dict
job_status_from_dict = JobStatus.from_dict(job_status_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


