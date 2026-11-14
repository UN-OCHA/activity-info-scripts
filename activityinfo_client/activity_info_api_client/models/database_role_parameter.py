from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Self, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="DatabaseRoleParameter")


@_attrs_define
class DatabaseRoleParameter:
    """Parameters defined for this role. Parameters can be referenced in filtering formulas.

    Attributes:
        parameter_id (str): This parameter's string id by which it will be referred to in formulas.
        label (str): Human-readable label for this parameter.
        range_ (str): The id of the form resource from which the parameter's values are looked up.
    """

    parameter_id: str
    label: str
    range_: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        parameter_id = self.parameter_id

        label = self.label

        range_ = self.range_

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "parameterId": parameter_id,
                "label": label,
                "range": range_,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        d = dict(src_dict)
        parameter_id = d.pop("parameterId")

        label = d.pop("label")

        range_ = d.pop("range")

        database_role_parameter = cls(
            parameter_id=parameter_id,
            label=label,
            range_=range_,
        )

        database_role_parameter.additional_properties = d
        return database_role_parameter

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
