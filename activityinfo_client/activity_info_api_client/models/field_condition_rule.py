from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Self, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="FieldConditionRule")


@_attrs_define
class FieldConditionRule:
    """An individual rule for a field-level condition associated with a Grant for a database resource.

    Attributes:
        object_type (str): The object type for this rule, e.g. 'FIELD'
        predicate_type (str): The predicate for this rule, e.g. 'IS' or 'ISNOT'
        value (str): The comparison value for this rule, e.g. the field id
    """

    object_type: str
    predicate_type: str
    value: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_type = self.object_type

        predicate_type = self.predicate_type

        value = self.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "objectType": object_type,
                "predicateType": predicate_type,
                "value": value,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        d = dict(src_dict)
        object_type = d.pop("objectType")

        predicate_type = d.pop("predicateType")

        value = d.pop("value")

        field_condition_rule = cls(
            object_type=object_type,
            predicate_type=predicate_type,
            value=value,
        )

        field_condition_rule.additional_properties = d
        return field_condition_rule

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
