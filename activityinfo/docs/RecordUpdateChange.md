# RecordUpdateChange


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**form_id** | **str** |  | 
**record_id** | **str** |  | 
**parent_record_id** | **str** |  | [optional] 
**deleted** | **bool** |  | [optional] [default to False]
**fields** | **Dict[str, object]** |  | 

## Example

```python
from client.models.record_update_change import RecordUpdateChange

# TODO update the JSON string below
json = "{}"
# create an instance of RecordUpdateChange from a JSON string
record_update_change_instance = RecordUpdateChange.from_json(json)
# print the JSON string representation of the object
print(RecordUpdateChange.to_json())

# convert the object into a dict
record_update_change_dict = record_update_change_instance.to_dict()
# create an instance of RecordUpdateChange from a dict
record_update_change_from_dict = RecordUpdateChange.from_dict(record_update_change_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


