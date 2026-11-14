from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.form_schema_element_type import FormSchemaElementType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.form_schema_element_parameter import FormSchemaElementParameter


T = TypeVar("T", bound="FormSchemaElement")


@_attrs_define
class FormSchemaElement:
    """
    Attributes:
        id (str): An immutable CUID for this field that is unique within the form
        label (str): A short human-friendly label for this field
        required (bool): If true, a value for this field must be provided Default: False.
        type_ (FormSchemaElementType): The field type.
        code (str | Unset): A developer-friendly code for this field that can be used in formulas and in the API. Must
            match the pattern ^[A-Za-z][A-Za-z0-9_]*
        description (str | Unset): An optional more detailed sub-label for this field
        relevance_condition (str | Unset): A boolean-valued ActivityInfo formula that determines when this field is
            'relevant'. Fields that are not relevant are not displayed during data entry and must be blank.
        validation_condition (str | Unset): A boolean-valued ActivityInfo formula that determines when this field's
            value is valid.
        data_entry_visible (bool | Unset): If false, this field is not shown in data entry Default: True.
        table_visible (bool | Unset): If false, this field is not shown by default in the table view Default: True.
        key (bool | Unset): If true, this field is part of the form's natural key fields whose combination must be
            unique within the form Default: False.
        unique (bool | Unset): If true, the value for this field must be unique across the form's records Default:
            False.
        read_only (bool | Unset): If true, the value for this field cannot be modified by any user (but it can be set by
            default on record add). Default: False.
        default_value (str | Unset): The default value for this field, set on record add
        default_value_formula (str | Unset): An ActivityInfo formula that generates the default value for this field,
            set on record add
        security_category_id (str | Unset):
        type_parameters (FormSchemaElementParameter | Unset):
    """

    id: str
    label: str
    type_: FormSchemaElementType
    required: bool = False
    code: str | Unset = UNSET
    description: str | Unset = UNSET
    relevance_condition: str | Unset = UNSET
    validation_condition: str | Unset = UNSET
    data_entry_visible: bool | Unset = True
    table_visible: bool | Unset = True
    key: bool | Unset = False
    unique: bool | Unset = False
    read_only: bool | Unset = False
    default_value: str | Unset = UNSET
    default_value_formula: str | Unset = UNSET
    security_category_id: str | Unset = UNSET
    type_parameters: FormSchemaElementParameter | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        label = self.label

        required = self.required

        type_ = self.type_.value

        code = self.code

        description = self.description

        relevance_condition = self.relevance_condition

        validation_condition = self.validation_condition

        data_entry_visible = self.data_entry_visible

        table_visible = self.table_visible

        key = self.key

        unique = self.unique

        read_only = self.read_only

        default_value = self.default_value

        default_value_formula = self.default_value_formula

        security_category_id = self.security_category_id

        type_parameters: dict[str, Any] | Unset = UNSET
        if not isinstance(self.type_parameters, Unset):
            type_parameters = self.type_parameters.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "label": label,
                "required": required,
                "type": type_,
            }
        )
        if code is not UNSET:
            field_dict["code"] = code
        if description is not UNSET:
            field_dict["description"] = description
        if relevance_condition is not UNSET:
            field_dict["relevanceCondition"] = relevance_condition
        if validation_condition is not UNSET:
            field_dict["validationCondition"] = validation_condition
        if data_entry_visible is not UNSET:
            field_dict["dataEntryVisible"] = data_entry_visible
        if table_visible is not UNSET:
            field_dict["tableVisible"] = table_visible
        if key is not UNSET:
            field_dict["key"] = key
        if unique is not UNSET:
            field_dict["unique"] = unique
        if read_only is not UNSET:
            field_dict["readOnly"] = read_only
        if default_value is not UNSET:
            field_dict["defaultValue"] = default_value
        if default_value_formula is not UNSET:
            field_dict["defaultValueFormula"] = default_value_formula
        if security_category_id is not UNSET:
            field_dict["securityCategoryId"] = security_category_id
        if type_parameters is not UNSET:
            field_dict["typeParameters"] = type_parameters

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.form_schema_element_parameter import FormSchemaElementParameter

        d = dict(src_dict)
        id = d.pop("id")

        label = d.pop("label")

        required = d.pop("required")

        type_ = FormSchemaElementType(d.pop("type"))

        code = d.pop("code", UNSET)

        description = d.pop("description", UNSET)

        relevance_condition = d.pop("relevanceCondition", UNSET)

        validation_condition = d.pop("validationCondition", UNSET)

        data_entry_visible = d.pop("dataEntryVisible", UNSET)

        table_visible = d.pop("tableVisible", UNSET)

        key = d.pop("key", UNSET)

        unique = d.pop("unique", UNSET)

        read_only = d.pop("readOnly", UNSET)

        default_value = d.pop("defaultValue", UNSET)

        default_value_formula = d.pop("defaultValueFormula", UNSET)

        security_category_id = d.pop("securityCategoryId", UNSET)

        _type_parameters = d.pop("typeParameters", UNSET)
        type_parameters: FormSchemaElementParameter | Unset
        if isinstance(_type_parameters, Unset):
            type_parameters = UNSET
        else:
            type_parameters = FormSchemaElementParameter.from_dict(_type_parameters)

        form_schema_element = cls(
            id=id,
            label=label,
            required=required,
            type_=type_,
            code=code,
            description=description,
            relevance_condition=relevance_condition,
            validation_condition=validation_condition,
            data_entry_visible=data_entry_visible,
            table_visible=table_visible,
            key=key,
            unique=unique,
            read_only=read_only,
            default_value=default_value,
            default_value_formula=default_value_formula,
            security_category_id=security_category_id,
            type_parameters=type_parameters,
        )

        form_schema_element.additional_properties = d
        return form_schema_element

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
