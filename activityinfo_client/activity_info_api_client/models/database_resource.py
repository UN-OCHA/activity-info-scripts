from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Self, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.database_resource_type import DatabaseResourceType
from ..models.database_resource_visibility import DatabaseResourceVisibility
from ..types import UNSET, Unset

T = TypeVar("T", bound="DatabaseResource")


@_attrs_define
class DatabaseResource:
    """A resource (folder, form or subform) that belongs to a database

    Attributes:
        id (str): The resource's id. Form and subform resources must have globally-unique ids (across all of
            ActivityInfo) but folder ids are only guaranteed to be unique within a single database.
        type_ (DatabaseResourceType | Unset): The resource type. Values: DATABASE, FOLDER, REPORT, FORM, SUB_FORM
        parent_id (str | Unset): The id of this resource's parent, for example, a folder id. If this is a top-level
            folder or form, then the parentId will be the id of the database itself.
        label (str | Unset): The resource's human-readable label.
        visibility (DatabaseResourceVisibility | Unset): The resource's visibility. Values: PUBLIC, PRIVATE, REFERENCE
        icon (str | Unset): Icon for the resource.
    """

    id: str
    type_: DatabaseResourceType | Unset = UNSET
    parent_id: str | Unset = UNSET
    label: str | Unset = UNSET
    visibility: DatabaseResourceVisibility | Unset = UNSET
    icon: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        type_: str | Unset = UNSET
        if not isinstance(self.type_, Unset):
            type_ = self.type_.value

        parent_id = self.parent_id

        label = self.label

        visibility: str | Unset = UNSET
        if not isinstance(self.visibility, Unset):
            visibility = self.visibility.value

        icon = self.icon

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
            }
        )
        if type_ is not UNSET:
            field_dict["type"] = type_
        if parent_id is not UNSET:
            field_dict["parentId"] = parent_id
        if label is not UNSET:
            field_dict["label"] = label
        if visibility is not UNSET:
            field_dict["visibility"] = visibility
        if icon is not UNSET:
            field_dict["icon"] = icon

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        d = dict(src_dict)
        id = d.pop("id")

        _type_ = d.pop("type", UNSET)
        type_: DatabaseResourceType | Unset
        if isinstance(_type_, Unset):
            type_ = UNSET
        else:
            type_ = DatabaseResourceType(_type_)

        parent_id = d.pop("parentId", UNSET)

        label = d.pop("label", UNSET)

        _visibility = d.pop("visibility", UNSET)
        visibility: DatabaseResourceVisibility | Unset
        if isinstance(_visibility, Unset):
            visibility = UNSET
        else:
            visibility = DatabaseResourceVisibility(_visibility)

        icon = d.pop("icon", UNSET)

        database_resource = cls(
            id=id,
            type_=type_,
            parent_id=parent_id,
            label=label,
            visibility=visibility,
            icon=icon,
        )

        database_resource.additional_properties = d
        return database_resource

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
