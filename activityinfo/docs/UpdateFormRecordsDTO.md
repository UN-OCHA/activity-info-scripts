# UpdateFormRecordsDTO


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**changes** | [**List[RecordUpdateDTO]**](RecordUpdateDTO.md) |  | 

## Example

```python
from client.models.update_form_records_dto import UpdateFormRecordsDTO

# TODO update the JSON string below
json = "{}"
# create an instance of UpdateFormRecordsDTO from a JSON string
update_form_records_dto_instance = UpdateFormRecordsDTO.from_json(json)
# print the JSON string representation of the object
print(UpdateFormRecordsDTO.to_json())

# convert the object into a dict
update_form_records_dto_dict = update_form_records_dto_instance.to_dict()
# create an instance of UpdateFormRecordsDTO from a dict
update_form_records_dto_from_dict = UpdateFormRecordsDTO.from_dict(update_form_records_dto_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


