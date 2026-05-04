# FormSchemaElementParameter


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**prefix_formula** | **str** | For fields of type &#39;serial&#39;, provides a formula that will be used to compute the prefix of the generated serial number. | [optional] 
**digits** | **int** | For fields of type &#39;serial&#39;, indicates how many digits long the generated serial number will be | [optional] 
**units** | **str** | For fields of type &#39;quantity&#39;, describes the units of the quantity | [optional] 
**aggregation** | **str** | For fields of type &#39;quantity&#39;, describes the aggregation method e.g. &#39;sum&#39;. Not currently used(?) | [optional] 
**input_mask** | **str** | For fields of type &#39;FREE_TEXT&#39; (user type &#39;Text&#39; or &#39;Barcode&#39;), provides a pattern-based input mask | [optional] 
**barcode** | **bool** | For fields of type &#39;FREE_TEXT&#39; (user type &#39;Text&#39; or &#39;Barcode&#39;), true when field is barcode | [optional] 
**formula** | **str** | For fields of type &#39;calculated&#39;, provides the formula for the field&#39;s value | [optional] 
**cardinality** | [**AnyOf**](AnyOf.md) | For fields of type &#39;enumerated&#39; (user type &#39;Single selection&#39; or &#39;Multiple selection&#39;), indicates whether single or multiple select | [optional] 
**values** | [**List[FormSchemaElementParameterValue]**](FormSchemaElementParameterValue.md) | For fields of type &#39;enumerated&#39; (user type &#39;Single selection&#39; or &#39;Multiple selection&#39;), provided the list of valid choices | [optional] 
**presentation** | **str** | For fields of type &#39;enumerated&#39; (user type &#39;Single selection&#39; or &#39;Multiple selection&#39;), indicate whether shown as dropdown or not. Only relevant for multiple selection, i.e. where cardinality: multiple | [optional] 
**range** | [**List[RangeInner]**](RangeInner.md) | For fields of type &#39;reference&#39; or &#39;multiselectreference&#39; (user type &#39;Reference&#39;, &#39;User&#39;, or &#39;Multiple Reference&#39;), provides the id of the form that this field references. For &#39;User&#39;, this reference is &#39;{dbid}@users&#39;. | [optional] 
**lookup_configs** | [**List[FormSchemaElementParameterLookup]**](FormSchemaElementParameterLookup.md) | For fields of type &#39;reference&#39;, specifies the lookup configuration for the field. | [optional] 
**form_id** | **str** | For fields of type &#39;subform&#39; and &#39;reversereference&#39;, provides the id of the form/subform that this field references | [optional] 
**field_id** | **str** | For fields of type &#39;reversereference&#39;, provides the id of the reference field on the form that this field references | [optional] 
**capture_methods** | **List[str]** | For fields of type &#39;attachment&#39;, indicates which types of attachments can be added. | [optional] 
**required_accuracy** | **int** | For fields of type &#39;geopoint&#39;, indicates the accuracy required when automatically specifying location. | [optional] 
**manual_entry_allowed** | **bool** | For fields of type &#39;geopoint&#39;, true when manual entry of location is allowed. | [optional] 
**indentation_level** | **int** | For fields of type &#39;section&#39;, indicates the level of indentation of the heading. | [optional] 
**translation_config** | [**FormSchemaElementParameterTranslation**](FormSchemaElementParameterTranslation.md) | For fields of type &#39;FREE_TEXT&#39;, &#39;NARRATIVE&#39; or &#39;calculated&#39;, the translation config specifies which other field this is a translation of, and into which language. | [optional] 

## Example

```python
from client.models.form_schema_element_parameter import FormSchemaElementParameter

# TODO update the JSON string below
json = "{}"
# create an instance of FormSchemaElementParameter from a JSON string
form_schema_element_parameter_instance = FormSchemaElementParameter.from_json(json)
# print the JSON string representation of the object
print(FormSchemaElementParameter.to_json())

# convert the object into a dict
form_schema_element_parameter_dict = form_schema_element_parameter_instance.to_dict()
# create an instance of FormSchemaElementParameter from a dict
form_schema_element_parameter_from_dict = FormSchemaElementParameter.from_dict(form_schema_element_parameter_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


