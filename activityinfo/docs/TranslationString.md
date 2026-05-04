# TranslationString


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** | The id of the string. This id is based on how the string is used in the database or form schema. | 
**original** | **str** | The original, untranslated, string. | 
**translated** | **str** | The translated string | 
**auto_translated** | **bool** | True if this translation was made automatically. | 

## Example

```python
from client.models.translation_string import TranslationString

# TODO update the JSON string below
json = "{}"
# create an instance of TranslationString from a JSON string
translation_string_instance = TranslationString.from_json(json)
# print the JSON string representation of the object
print(TranslationString.to_json())

# convert the object into a dict
translation_string_dict = translation_string_instance.to_dict()
# create an instance of TranslationString from a dict
translation_string_from_dict = TranslationString.from_dict(translation_string_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


