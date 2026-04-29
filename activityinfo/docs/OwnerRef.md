# OwnerRef


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** |  | 
**name** | **str** |  | 
**email** | **str** |  | 

## Example

```python
from client.models.owner_ref import OwnerRef

# TODO update the JSON string below
json = "{}"
# create an instance of OwnerRef from a JSON string
owner_ref_instance = OwnerRef.from_json(json)
# print the JSON string representation of the object
print(OwnerRef.to_json())

# convert the object into a dict
owner_ref_dict = owner_ref_instance.to_dict()
# create an instance of OwnerRef from a dict
owner_ref_from_dict = OwnerRef.from_dict(owner_ref_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


