# RecordUpdateDTO


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**form_id** | **str** |  | 
**record_id** | **str** |  | 
**parent_record_id** | **str** |  | [optional] 
**deleted** | **bool** |  | [optional] 
**fields** | **Dict[str, object]** |  | 

## Example

```python
from client.models.record_update_dto import RecordUpdateDTO

# TODO update the JSON string below
json = "{}"
# create an instance of RecordUpdateDTO from a JSON string
record_update_dto_instance = RecordUpdateDTO.from_json(json)
# print the JSON string representation of the object
print(RecordUpdateDTO.to_json())

# convert the object into a dict
record_update_dto_dict = record_update_dto_instance.to_dict()
# create an instance of RecordUpdateDTO from a dict
record_update_dto_from_dict = RecordUpdateDTO.from_dict(record_update_dto_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


