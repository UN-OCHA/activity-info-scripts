from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Self, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.form_permissions import FormPermissions
    from ..models.form_schema import FormSchema


T = TypeVar("T", bound="Form")


@_attrs_define
class Form:
    """
    Attributes:
        id (str): The id of the form
        schema (FormSchema):
        schema_version (int):
        permissions (FormPermissions):
        visible (bool | Unset): True if the form is visible to the authenticated user
        deleted (bool | Unset): True if the form has been deleted
        suspended (bool | Unset): True if the form belongs to a suspended database.
    """

    id: str
    schema: FormSchema
    schema_version: int
    permissions: FormPermissions
    visible: bool | Unset = UNSET
    deleted: bool | Unset = UNSET
    suspended: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        schema = self.schema.to_dict()

        schema_version = self.schema_version

        permissions = self.permissions.to_dict()

        visible = self.visible

        deleted = self.deleted

        suspended = self.suspended

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "schema": schema,
                "schemaVersion": schema_version,
                "permissions": permissions,
            }
        )
        if visible is not UNSET:
            field_dict["visible"] = visible
        if deleted is not UNSET:
            field_dict["deleted"] = deleted
        if suspended is not UNSET:
            field_dict["suspended"] = suspended

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        from ..models.form_permissions import FormPermissions
        from ..models.form_schema import FormSchema

        d = dict(src_dict)
        id = d.pop("id")

        schema = FormSchema.from_dict(d.pop("schema"))

        schema_version = d.pop("schemaVersion")

        permissions = FormPermissions.from_dict(d.pop("permissions"))

        visible = d.pop("visible", UNSET)

        deleted = d.pop("deleted", UNSET)

        suspended = d.pop("suspended", UNSET)

        form = cls(
            id=id,
            schema=schema,
            schema_version=schema_version,
            permissions=permissions,
            visible=visible,
            deleted=deleted,
            suspended=suspended,
        )

        form.additional_properties = d
        return form

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
