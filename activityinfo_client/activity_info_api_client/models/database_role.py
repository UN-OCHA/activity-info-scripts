from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Self, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.database_role_filter import DatabaseRoleFilter
    from ..models.database_role_parameter import DatabaseRoleParameter
    from ..models.grant import Grant
    from ..models.permission import Permission


T = TypeVar("T", bound="DatabaseRole")


@_attrs_define
class DatabaseRole:
    """
    Attributes:
        id (str): This role's id
        label (str): This role's human-readable label
        permissions (list[Permission] | Unset): Permissions granted this role. Applicable for legacy roles, for grant
            based roles it will be limited to permissions that are NOT related to resources, like MANAGE_USERS, MANAGE_ROLES
        parameters (list[DatabaseRoleParameter] | Unset): Parameters defined for this role. Parameters can be referenced
            in filtering formulas.
        filters (list[DatabaseRoleFilter] | Unset): Pre-defined filters. Role filters allow other users to choose
            filters for permissions without having to write formulas themselves. -- NOTEWORTHY - only used by legacy roles
        grants (list[Grant] | Unset):
        version (int | Unset):  Default: 0.
        grant_based (bool | Unset):  Default: True.
    """

    id: str
    label: str
    permissions: list[Permission] | Unset = UNSET
    parameters: list[DatabaseRoleParameter] | Unset = UNSET
    filters: list[DatabaseRoleFilter] | Unset = UNSET
    grants: list[Grant] | Unset = UNSET
    version: int | Unset = 0
    grant_based: bool | Unset = True
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        label = self.label

        permissions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.permissions, Unset):
            permissions = []
            for permissions_item_data in self.permissions:
                permissions_item = permissions_item_data.to_dict()
                permissions.append(permissions_item)

        parameters: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.parameters, Unset):
            parameters = []
            for parameters_item_data in self.parameters:
                parameters_item = parameters_item_data.to_dict()
                parameters.append(parameters_item)

        filters: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.filters, Unset):
            filters = []
            for filters_item_data in self.filters:
                filters_item = filters_item_data.to_dict()
                filters.append(filters_item)

        grants: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.grants, Unset):
            grants = []
            for grants_item_data in self.grants:
                grants_item = grants_item_data.to_dict()
                grants.append(grants_item)

        version = self.version

        grant_based = self.grant_based

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "label": label,
            }
        )
        if permissions is not UNSET:
            field_dict["permissions"] = permissions
        if parameters is not UNSET:
            field_dict["parameters"] = parameters
        if filters is not UNSET:
            field_dict["filters"] = filters
        if grants is not UNSET:
            field_dict["grants"] = grants
        if version is not UNSET:
            field_dict["version"] = version
        if grant_based is not UNSET:
            field_dict["grantBased"] = grant_based

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        from ..models.database_role_filter import DatabaseRoleFilter
        from ..models.database_role_parameter import DatabaseRoleParameter
        from ..models.grant import Grant
        from ..models.permission import Permission

        d = dict(src_dict)
        id = d.pop("id")

        label = d.pop("label")

        _permissions = d.pop("permissions", UNSET)
        permissions: list[Permission] | Unset = UNSET
        if _permissions is not UNSET:
            permissions = []
            for permissions_item_data in _permissions:
                permissions_item = Permission.from_dict(permissions_item_data)

                permissions.append(permissions_item)

        _parameters = d.pop("parameters", UNSET)
        parameters: list[DatabaseRoleParameter] | Unset = UNSET
        if _parameters is not UNSET:
            parameters = []
            for parameters_item_data in _parameters:
                parameters_item = DatabaseRoleParameter.from_dict(parameters_item_data)

                parameters.append(parameters_item)

        _filters = d.pop("filters", UNSET)
        filters: list[DatabaseRoleFilter] | Unset = UNSET
        if _filters is not UNSET:
            filters = []
            for filters_item_data in _filters:
                filters_item = DatabaseRoleFilter.from_dict(filters_item_data)

                filters.append(filters_item)

        _grants = d.pop("grants", UNSET)
        grants: list[Grant] | Unset = UNSET
        if _grants is not UNSET:
            grants = []
            for grants_item_data in _grants:
                grants_item = Grant.from_dict(grants_item_data)

                grants.append(grants_item)

        version = d.pop("version", UNSET)

        grant_based = d.pop("grantBased", UNSET)

        database_role = cls(
            id=id,
            label=label,
            permissions=permissions,
            parameters=parameters,
            filters=filters,
            grants=grants,
            version=version,
            grant_based=grant_based,
        )

        database_role.additional_properties = d
        return database_role

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
