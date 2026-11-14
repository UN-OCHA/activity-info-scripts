from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Self, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="FormSchemaElementParameterLookup")


@_attrs_define
class FormSchemaElementParameterLookup:
    """
    Attributes:
        id (str): The immutable CUID of this lookup item. Must match the format [a-z][a-z0-9]{0,25} and be unique within
            this field.
        formula (str): An ActivityInfo formula specifying the field or expression being looked up.
        lookup_label (str): Human-readable label for this lookup item.
    """

    id: str
    formula: str
    lookup_label: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        formula = self.formula

        lookup_label = self.lookup_label

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "formula": formula,
                "lookupLabel": lookup_label,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        d = dict(src_dict)
        id = d.pop("id")

        formula = d.pop("formula")

        lookup_label = d.pop("lookupLabel")

        form_schema_element_parameter_lookup = cls(
            id=id,
            formula=formula,
            lookup_label=lookup_label,
        )

        form_schema_element_parameter_lookup.additional_properties = d
        return form_schema_element_parameter_lookup

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
