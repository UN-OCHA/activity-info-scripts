# UpdateTranslationsRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**strings** | [**List[TranslationString]**](TranslationString.md) |  | 

## Example

```python
from client.models.update_translations_request import UpdateTranslationsRequest

# TODO update the JSON string below
json = "{}"
# create an instance of UpdateTranslationsRequest from a JSON string
update_translations_request_instance = UpdateTranslationsRequest.from_json(json)
# print the JSON string representation of the object
print(UpdateTranslationsRequest.to_json())

# convert the object into a dict
update_translations_request_dict = update_translations_request_instance.to_dict()
# create an instance of UpdateTranslationsRequest from a dict
update_translations_request_from_dict = UpdateTranslationsRequest.from_dict(update_translations_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


