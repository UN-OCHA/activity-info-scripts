# UpdateDatabaseTranslationsDTO


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**strings** | [**List[DatabaseTranslation]**](DatabaseTranslation.md) |  | 

## Example

```python
from client.models.update_database_translations_dto import UpdateDatabaseTranslationsDTO

# TODO update the JSON string below
json = "{}"
# create an instance of UpdateDatabaseTranslationsDTO from a JSON string
update_database_translations_dto_instance = UpdateDatabaseTranslationsDTO.from_json(json)
# print the JSON string representation of the object
print(UpdateDatabaseTranslationsDTO.to_json())

# convert the object into a dict
update_database_translations_dto_dict = update_database_translations_dto_instance.to_dict()
# create an instance of UpdateDatabaseTranslationsDTO from a dict
update_database_translations_dto_from_dict = UpdateDatabaseTranslationsDTO.from_dict(update_database_translations_dto_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


