# TranslationDictionary


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | [**TranslationDictionaryId**](TranslationDictionaryId.md) | The id of the dictionary | 
**version** | **int** | The current version number of the dictionary. The version number is incremented each time the dictionary is updated. | 
**language** | **str** | The two-letter ISO 639-1 code for the language, e.g. &#39;en&#39; for English. | 
**translated_strings** | [**List[TranslationString]**](TranslationString.md) | The list of translated strings. | 

## Example

```python
from client.models.translation_dictionary import TranslationDictionary

# TODO update the JSON string below
json = "{}"
# create an instance of TranslationDictionary from a JSON string
translation_dictionary_instance = TranslationDictionary.from_json(json)
# print the JSON string representation of the object
print(TranslationDictionary.to_json())

# convert the object into a dict
translation_dictionary_dict = translation_dictionary_instance.to_dict()
# create an instance of TranslationDictionary from a dict
translation_dictionary_from_dict = TranslationDictionary.from_dict(translation_dictionary_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


