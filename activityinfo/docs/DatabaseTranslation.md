# DatabaseTranslation


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** |  | 
**original** | **str** |  | 
**translated** | **str** |  | 
**auto_translated** | **bool** |  | 

## Example

```python
from client.models.database_translation import DatabaseTranslation

# TODO update the JSON string below
json = "{}"
# create an instance of DatabaseTranslation from a JSON string
database_translation_instance = DatabaseTranslation.from_json(json)
# print the JSON string representation of the object
print(DatabaseTranslation.to_json())

# convert the object into a dict
database_translation_dict = database_translation_instance.to_dict()
# create an instance of DatabaseTranslation from a dict
database_translation_from_dict = DatabaseTranslation.from_dict(database_translation_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


