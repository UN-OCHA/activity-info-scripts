# TranslationDictionaryId


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**database_id** | **str** | The database to which this dictionary belongs | 
**dictionary_id** | **str** | The dictionary id. This will either be \&quot;database\&quot; or \&quot;form/{formId}.\&quot; | 

## Example

```python
from client.models.translation_dictionary_id import TranslationDictionaryId

# TODO update the JSON string below
json = "{}"
# create an instance of TranslationDictionaryId from a JSON string
translation_dictionary_id_instance = TranslationDictionaryId.from_json(json)
# print the JSON string representation of the object
print(TranslationDictionaryId.to_json())

# convert the object into a dict
translation_dictionary_id_dict = translation_dictionary_id_instance.to_dict()
# create an instance of TranslationDictionaryId from a dict
translation_dictionary_id_from_dict = TranslationDictionaryId.from_dict(translation_dictionary_id_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


