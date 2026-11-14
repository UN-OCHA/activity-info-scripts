from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.grant import Grant
    from ..models.user_role import UserRole


T = TypeVar("T", bound="AddUserRequest")


@_attrs_define
class AddUserRequest:
    """
    Attributes:
        email (str): The email of the user to add
        name (str): The name of the user to add. This is only used in the email invitation to the user if there is no
            existing ActivityInfo account with this email address.
        locale (str): The locale to use when sending an email invite to the user, expressed as the two-letter ISO 639-1
            code for the language, e.g. 'en' for English.
        role (UserRole):
        grants (list[Grant] | Unset): Additional permissions to grant to the user.
    """

    email: str
    name: str
    locale: str
    role: UserRole
    grants: list[Grant] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        email = self.email

        name = self.name

        locale = self.locale

        role = self.role.to_dict()

        grants: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.grants, Unset):
            grants = []
            for grants_item_data in self.grants:
                grants_item = grants_item_data.to_dict()
                grants.append(grants_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "email": email,
                "name": name,
                "locale": locale,
                "role": role,
            }
        )
        if grants is not UNSET:
            field_dict["grants"] = grants

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.grant import Grant
        from ..models.user_role import UserRole

        d = dict(src_dict)
        email = d.pop("email")

        name = d.pop("name")

        locale = d.pop("locale")

        role = UserRole.from_dict(d.pop("role"))

        _grants = d.pop("grants", UNSET)
        grants: list[Grant] | Unset = UNSET
        if _grants is not UNSET:
            grants = []
            for grants_item_data in _grants:
                grants_item = Grant.from_dict(grants_item_data)

                grants.append(grants_item)

        add_user_request = cls(
            email=email,
            name=name,
            locale=locale,
            role=role,
            grants=grants,
        )

        add_user_request.additional_properties = d
        return add_user_request

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
