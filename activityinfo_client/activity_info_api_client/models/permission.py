from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Self, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.permission_operation import PermissionOperation
from ..types import UNSET, Unset

T = TypeVar("T", bound="Permission")


@_attrs_define
class Permission:
    """An individual operation (add/edit/view) and any associated record-level condition, either associated with a Grant
    for a specific database resource, or assigned to a role directly for higher-level database operations.

        Attributes:
            operation (PermissionOperation): One of a number of predetermined values
            filter_ (str | Unset): A formula which restricts a permission to specific records. Only record-level permissions
                can be filtered.
            security_categories (list[str] | Unset): The security categories that have been defined for this permission.
    """

    operation: PermissionOperation
    filter_: str | Unset = UNSET
    security_categories: list[str] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        operation = self.operation.value

        filter_ = self.filter_

        security_categories: list[str] | Unset = UNSET
        if not isinstance(self.security_categories, Unset):
            security_categories = self.security_categories

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "operation": operation,
            }
        )
        if filter_ is not UNSET:
            field_dict["filter"] = filter_
        if security_categories is not UNSET:
            field_dict["securityCategories"] = security_categories

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        d = dict(src_dict)
        operation = PermissionOperation(d.pop("operation"))

        filter_ = d.pop("filter", UNSET)

        security_categories = cast(list[str], d.pop("securityCategories", UNSET))

        permission = cls(
            operation=operation,
            filter_=filter_,
            security_categories=security_categories,
        )

        permission.additional_properties = d
        return permission

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
