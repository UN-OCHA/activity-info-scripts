# TypeParameterLookupConfig


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** |  | 
**formula** | **str** |  | [optional] 
**lookup_label** | **str** |  | [optional] 

## Example

```python
from client.models.type_parameter_lookup_config import TypeParameterLookupConfig

# TODO update the JSON string below
json = "{}"
# create an instance of TypeParameterLookupConfig from a JSON string
type_parameter_lookup_config_instance = TypeParameterLookupConfig.from_json(json)
# print the JSON string representation of the object
print(TypeParameterLookupConfig.to_json())

# convert the object into a dict
type_parameter_lookup_config_dict = type_parameter_lookup_config_instance.to_dict()
# create an instance of TypeParameterLookupConfig from a dict
type_parameter_lookup_config_from_dict = TypeParameterLookupConfig.from_dict(type_parameter_lookup_config_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


