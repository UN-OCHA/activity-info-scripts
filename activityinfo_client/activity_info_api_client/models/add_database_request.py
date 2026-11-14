from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Self, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="AddDatabaseRequest")


@_attrs_define
class AddDatabaseRequest:
    """
    Attributes:
        id (str): The id of the new database.
        label (str): Human readable name of the database.
        template_id (str | Unset): The id of the template to use in creating the new database. The the two templates
            currently supported are "reporting" (Multi-partner reporting) and "casemanagement." If ommitted, a blank
            database with three simple roles will be added.
        description (str | Unset): An optional, longer description for the database.
    """

    id: str
    label: str
    template_id: str | Unset = UNSET
    description: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        label = self.label

        template_id = self.template_id

        description = self.description

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "label": label,
            }
        )
        if template_id is not UNSET:
            field_dict["templateId"] = template_id
        if description is not UNSET:
            field_dict["description"] = description

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        d = dict(src_dict)
        id = d.pop("id")

        label = d.pop("label")

        template_id = d.pop("templateId", UNSET)

        description = d.pop("description", UNSET)

        add_database_request = cls(
            id=id,
            label=label,
            template_id=template_id,
            description=description,
        )

        add_database_request.additional_properties = d
        return add_database_request

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
