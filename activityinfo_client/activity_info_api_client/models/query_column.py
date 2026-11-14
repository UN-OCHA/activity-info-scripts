from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="QueryColumn")


@_attrs_define
class QueryColumn:
    """
    Attributes:
        id (str): The id of the column. This is the column name used to identify the column in the resulting table.
        formula (str | Unset): An ActivityInfo formula that calculates the value shown in the column. formula and
            expression are interchangeable, but formula is given precedence.
        expression (str | Unset): An ActivityInfo formula that calculates the value shown in the column. formula and
            expression are interchangeable, but formula is given precedence.
    """

    id: str
    formula: str | Unset = UNSET
    expression: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        formula = self.formula

        expression = self.expression

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
            }
        )
        if formula is not UNSET:
            field_dict["formula"] = formula
        if expression is not UNSET:
            field_dict["expression"] = expression

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        formula = d.pop("formula", UNSET)

        expression = d.pop("expression", UNSET)

        query_column = cls(
            id=id,
            formula=formula,
            expression=expression,
        )

        query_column.additional_properties = d
        return query_column

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
