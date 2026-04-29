# DatabaseTranslations


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | [**DatabaseTranslationsID**](DatabaseTranslationsID.md) |  | 
**version** | **int** |  | 
**language** | **str** |  | 
**translated_strings** | [**List[DatabaseTranslation]**](DatabaseTranslation.md) |  | 

## Example

```python
from client.models.database_translations import DatabaseTranslations

# TODO update the JSON string below
json = "{}"
# create an instance of DatabaseTranslations from a JSON string
database_translations_instance = DatabaseTranslations.from_json(json)
# print the JSON string representation of the object
print(DatabaseTranslations.to_json())

# convert the object into a dict
database_translations_dict = database_translations_instance.to_dict()
# create an instance of DatabaseTranslations from a dict
database_translations_from_dict = DatabaseTranslations.from_dict(database_translations_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


