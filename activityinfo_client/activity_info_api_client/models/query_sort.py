from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Self, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.query_sort_dir import QuerySortDir

T = TypeVar("T", bound="QuerySort")


@_attrs_define
class QuerySort:
    """
    Attributes:
        formula (str): The formula on which to sort.
        dir_ (QuerySortDir): The direction to sort.
    """

    formula: str
    dir_: QuerySortDir
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        formula = self.formula

        dir_ = self.dir_.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "formula": formula,
                "dir": dir_,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        d = dict(src_dict)
        formula = d.pop("formula")

        dir_ = QuerySortDir(d.pop("dir"))

        query_sort = cls(
            formula=formula,
            dir_=dir_,
        )

        query_sort.additional_properties = d
        return query_sort

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
