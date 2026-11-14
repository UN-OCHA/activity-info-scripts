from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.form_schema_element_parameter_capture_methods_item import FormSchemaElementParameterCaptureMethodsItem
from ..models.form_schema_element_parameter_cardinality import FormSchemaElementParameterCardinality
from ..models.form_schema_element_parameter_presentation import FormSchemaElementParameterPresentation
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.form_schema_element_parameter_lookup import FormSchemaElementParameterLookup
    from ..models.form_schema_element_parameter_range_item import FormSchemaElementParameterRangeItem
    from ..models.form_schema_element_parameter_translation import FormSchemaElementParameterTranslation
    from ..models.form_schema_element_parameter_value import FormSchemaElementParameterValue


T = TypeVar("T", bound="FormSchemaElementParameter")


@_attrs_define
class FormSchemaElementParameter:
    """
    Attributes:
        prefix_formula (str | Unset): For fields of type 'serial', provides a formula that will be used to compute the
            prefix of the generated serial number.
        digits (int | Unset): For fields of type 'serial', indicates how many digits long the generated serial number
            will be
        units (str | Unset): For fields of type 'quantity', describes the units of the quantity
        aggregation (str | Unset): For fields of type 'quantity', describes the aggregation method e.g. 'sum'. Not
            currently used(?)
        input_mask (str | Unset): For fields of type 'FREE_TEXT' (user type 'Text' or 'Barcode'), provides a pattern-
            based input mask
        barcode (bool | Unset): For fields of type 'FREE_TEXT' (user type 'Text' or 'Barcode'), true when field is
            barcode
        formula (str | Unset): For fields of type 'calculated', provides the formula for the field's value
        cardinality (FormSchemaElementParameterCardinality | Unset): For fields of type 'enumerated' (user type 'Single
            selection' or 'Multiple selection'), indicates whether single or multiple select
        values (list[FormSchemaElementParameterValue] | Unset): For fields of type 'enumerated' (user type 'Single
            selection' or 'Multiple selection'), provided the list of valid choices
        presentation (FormSchemaElementParameterPresentation | Unset): For fields of type 'enumerated' (user type
            'Single selection' or 'Multiple selection'), indicate whether shown as dropdown or not. Only relevant for
            multiple selection, i.e. where cardinality: multiple
        range_ (list[FormSchemaElementParameterRangeItem] | Unset): For fields of type 'reference' or
            'multiselectreference' (user type 'Reference', 'User', or 'Multiple Reference'), provides the id of the form
            that this field references. For 'User', this reference is '{dbid}@users'.
        lookup_configs (list[FormSchemaElementParameterLookup] | Unset): For fields of type 'reference', specifies the
            lookup configuration for the field.
        form_id (str | Unset): For fields of type 'subform' and 'reversereference', provides the id of the form/subform
            that this field references
        field_id (str | Unset): For fields of type 'reversereference', provides the id of the reference field on the
            form that this field references
        capture_methods (list[FormSchemaElementParameterCaptureMethodsItem] | Unset): For fields of type 'attachment',
            indicates which types of attachments can be added.
        required_accuracy (int | Unset): For fields of type 'geopoint', indicates the accuracy required when
            automatically specifying location.
        manual_entry_allowed (bool | Unset): For fields of type 'geopoint', true when manual entry of location is
            allowed.
        indentation_level (int | Unset): For fields of type 'section', indicates the level of indentation of the
            heading.
        translation_config (FormSchemaElementParameterTranslation | Unset):
    """

    prefix_formula: str | Unset = UNSET
    digits: int | Unset = UNSET
    units: str | Unset = UNSET
    aggregation: str | Unset = UNSET
    input_mask: str | Unset = UNSET
    barcode: bool | Unset = UNSET
    formula: str | Unset = UNSET
    cardinality: FormSchemaElementParameterCardinality | Unset = UNSET
    values: list[FormSchemaElementParameterValue] | Unset = UNSET
    presentation: FormSchemaElementParameterPresentation | Unset = UNSET
    range_: list[FormSchemaElementParameterRangeItem] | Unset = UNSET
    lookup_configs: list[FormSchemaElementParameterLookup] | Unset = UNSET
    form_id: str | Unset = UNSET
    field_id: str | Unset = UNSET
    capture_methods: list[FormSchemaElementParameterCaptureMethodsItem] | Unset = UNSET
    required_accuracy: int | Unset = UNSET
    manual_entry_allowed: bool | Unset = UNSET
    indentation_level: int | Unset = UNSET
    translation_config: FormSchemaElementParameterTranslation | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        prefix_formula = self.prefix_formula

        digits = self.digits

        units = self.units

        aggregation = self.aggregation

        input_mask = self.input_mask

        barcode = self.barcode

        formula = self.formula

        cardinality: str | Unset = UNSET
        if not isinstance(self.cardinality, Unset):
            cardinality = self.cardinality.value

        values: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.values, Unset):
            values = []
            for values_item_data in self.values:
                values_item = values_item_data.to_dict()
                values.append(values_item)

        presentation: str | Unset = UNSET
        if not isinstance(self.presentation, Unset):
            presentation = self.presentation.value

        range_: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.range_, Unset):
            range_ = []
            for range_item_data in self.range_:
                range_item = range_item_data.to_dict()
                range_.append(range_item)

        lookup_configs: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.lookup_configs, Unset):
            lookup_configs = []
            for lookup_configs_item_data in self.lookup_configs:
                lookup_configs_item = lookup_configs_item_data.to_dict()
                lookup_configs.append(lookup_configs_item)

        form_id = self.form_id

        field_id = self.field_id

        capture_methods: list[str] | Unset = UNSET
        if not isinstance(self.capture_methods, Unset):
            capture_methods = []
            for capture_methods_item_data in self.capture_methods:
                capture_methods_item = capture_methods_item_data.value
                capture_methods.append(capture_methods_item)

        required_accuracy = self.required_accuracy

        manual_entry_allowed = self.manual_entry_allowed

        indentation_level = self.indentation_level

        translation_config: dict[str, Any] | Unset = UNSET
        if not isinstance(self.translation_config, Unset):
            translation_config = self.translation_config.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if prefix_formula is not UNSET:
            field_dict["prefixFormula"] = prefix_formula
        if digits is not UNSET:
            field_dict["digits"] = digits
        if units is not UNSET:
            field_dict["units"] = units
        if aggregation is not UNSET:
            field_dict["aggregation"] = aggregation
        if input_mask is not UNSET:
            field_dict["inputMask"] = input_mask
        if barcode is not UNSET:
            field_dict["barcode"] = barcode
        if formula is not UNSET:
            field_dict["formula"] = formula
        if cardinality is not UNSET:
            field_dict["cardinality"] = cardinality
        if values is not UNSET:
            field_dict["values"] = values
        if presentation is not UNSET:
            field_dict["presentation"] = presentation
        if range_ is not UNSET:
            field_dict["range"] = range_
        if lookup_configs is not UNSET:
            field_dict["lookupConfigs"] = lookup_configs
        if form_id is not UNSET:
            field_dict["formId"] = form_id
        if field_id is not UNSET:
            field_dict["fieldId"] = field_id
        if capture_methods is not UNSET:
            field_dict["captureMethods"] = capture_methods
        if required_accuracy is not UNSET:
            field_dict["requiredAccuracy"] = required_accuracy
        if manual_entry_allowed is not UNSET:
            field_dict["manualEntryAllowed"] = manual_entry_allowed
        if indentation_level is not UNSET:
            field_dict["indentationLevel"] = indentation_level
        if translation_config is not UNSET:
            field_dict["translationConfig"] = translation_config

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.form_schema_element_parameter_lookup import FormSchemaElementParameterLookup
        from ..models.form_schema_element_parameter_range_item import FormSchemaElementParameterRangeItem
        from ..models.form_schema_element_parameter_translation import FormSchemaElementParameterTranslation
        from ..models.form_schema_element_parameter_value import FormSchemaElementParameterValue

        d = dict(src_dict)
        prefix_formula = d.pop("prefixFormula", UNSET)

        digits = d.pop("digits", UNSET)

        units = d.pop("units", UNSET)

        aggregation = d.pop("aggregation", UNSET)

        input_mask = d.pop("inputMask", UNSET)

        barcode = d.pop("barcode", UNSET)

        formula = d.pop("formula", UNSET)

        _cardinality = d.pop("cardinality", UNSET)
        cardinality: FormSchemaElementParameterCardinality | Unset
        if isinstance(_cardinality, Unset):
            cardinality = UNSET
        else:
            cardinality = FormSchemaElementParameterCardinality(_cardinality)

        _values = d.pop("values", UNSET)
        values: list[FormSchemaElementParameterValue] | Unset = UNSET
        if _values is not UNSET:
            values = []
            for values_item_data in _values:
                values_item = FormSchemaElementParameterValue.from_dict(values_item_data)

                values.append(values_item)

        _presentation = d.pop("presentation", UNSET)
        presentation: FormSchemaElementParameterPresentation | Unset
        if isinstance(_presentation, Unset):
            presentation = UNSET
        else:
            presentation = FormSchemaElementParameterPresentation(_presentation)

        _range_ = d.pop("range", UNSET)
        range_: list[FormSchemaElementParameterRangeItem] | Unset = UNSET
        if _range_ is not UNSET:
            range_ = []
            for range_item_data in _range_:
                range_item = FormSchemaElementParameterRangeItem.from_dict(range_item_data)

                range_.append(range_item)

        _lookup_configs = d.pop("lookupConfigs", UNSET)
        lookup_configs: list[FormSchemaElementParameterLookup] | Unset = UNSET
        if _lookup_configs is not UNSET:
            lookup_configs = []
            for lookup_configs_item_data in _lookup_configs:
                lookup_configs_item = FormSchemaElementParameterLookup.from_dict(lookup_configs_item_data)

                lookup_configs.append(lookup_configs_item)

        form_id = d.pop("formId", UNSET)

        field_id = d.pop("fieldId", UNSET)

        _capture_methods = d.pop("captureMethods", UNSET)
        capture_methods: list[FormSchemaElementParameterCaptureMethodsItem] | Unset = UNSET
        if _capture_methods is not UNSET:
            capture_methods = []
            for capture_methods_item_data in _capture_methods:
                capture_methods_item = FormSchemaElementParameterCaptureMethodsItem(capture_methods_item_data)

                capture_methods.append(capture_methods_item)

        required_accuracy = d.pop("requiredAccuracy", UNSET)

        manual_entry_allowed = d.pop("manualEntryAllowed", UNSET)

        indentation_level = d.pop("indentationLevel", UNSET)

        _translation_config = d.pop("translationConfig", UNSET)
        translation_config: FormSchemaElementParameterTranslation | Unset
        if isinstance(_translation_config, Unset):
            translation_config = UNSET
        else:
            translation_config = FormSchemaElementParameterTranslation.from_dict(_translation_config)

        form_schema_element_parameter = cls(
            prefix_formula=prefix_formula,
            digits=digits,
            units=units,
            aggregation=aggregation,
            input_mask=input_mask,
            barcode=barcode,
            formula=formula,
            cardinality=cardinality,
            values=values,
            presentation=presentation,
            range_=range_,
            lookup_configs=lookup_configs,
            form_id=form_id,
            field_id=field_id,
            capture_methods=capture_methods,
            required_accuracy=required_accuracy,
            manual_entry_allowed=manual_entry_allowed,
            indentation_level=indentation_level,
            translation_config=translation_config,
        )

        form_schema_element_parameter.additional_properties = d
        return form_schema_element_parameter

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
