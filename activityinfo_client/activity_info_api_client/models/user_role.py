from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.user_role_parameters import UserRoleParameters


T = TypeVar("T", bound="UserRole")


@_attrs_define
class UserRole:
    """
    Attributes:
        id (str): The id of the role assigned to this user.
        parameters (UserRoleParameters | Unset): The user's values of the parameters defined for this role.
        resources (list[str] | Unset): The role's optional resources (database, folder, forms, or subforms) granted to
            this user.
    """

    id: str
    parameters: UserRoleParameters | Unset = UNSET
    resources: list[str] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        parameters: dict[str, Any] | Unset = UNSET
        if not isinstance(self.parameters, Unset):
            parameters = self.parameters.to_dict()

        resources: list[str] | Unset = UNSET
        if not isinstance(self.resources, Unset):
            resources = self.resources

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
            }
        )
        if parameters is not UNSET:
            field_dict["parameters"] = parameters
        if resources is not UNSET:
            field_dict["resources"] = resources

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.user_role_parameters import UserRoleParameters

        d = dict(src_dict)
        id = d.pop("id")

        _parameters = d.pop("parameters", UNSET)
        parameters: UserRoleParameters | Unset
        if isinstance(_parameters, Unset):
            parameters = UNSET
        else:
            parameters = UserRoleParameters.from_dict(_parameters)

        resources = cast(list[str], d.pop("resources", UNSET))

        user_role = cls(
            id=id,
            parameters=parameters,
            resources=resources,
        )

        user_role.additional_properties = d
        return user_role

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
