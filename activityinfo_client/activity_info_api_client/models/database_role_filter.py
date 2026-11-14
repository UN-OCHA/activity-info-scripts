from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="DatabaseRoleFilter")


@_attrs_define
class DatabaseRoleFilter:
    """Pre-defined filters. Role filters allow other users to choose filters for permissions without having to write
    formulas themselves. -- NOTEWORTHY - only used by legacy roles.

        Attributes:
            id (str): This filter's id.
            label (str): This filter's human-readable label.
            filter_ (str): A formula that can be used to filter a record-level permission.
    """

    id: str
    label: str
    filter_: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        label = self.label

        filter_ = self.filter_

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "label": label,
                "filter": filter_,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        label = d.pop("label")

        filter_ = d.pop("filter")

        database_role_filter = cls(
            id=id,
            label=label,
            filter_=filter_,
        )

        database_role_filter.additional_properties = d
        return database_role_filter

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
